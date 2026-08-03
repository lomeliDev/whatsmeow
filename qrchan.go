// Copyright (c) 2022 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type QRChannelItem struct {
	// The type of event, "code" for new QR codes (see Code field) and "error" for pairing errors (see Error) field.
	// For non-code/error events, you can just compare the whole item to the event variables (like QRChannelSuccess).
	Event string
	// If the item is a pair error, then this field contains the error message.
	Error error
	// If the item is a new code, then this field contains the raw data.
	Code string
	// The timeout after which the next code will be sent down the channel.
	Timeout time.Duration

	PasskeyRequest      *events.PairPasskeyRequest
	PasskeyConfirmation *events.PairPasskeyConfirmation
}

const QRChannelEventCode = "code"
const QRChannelEventError = "error"
const QRChannelEventPasskeyRequest = "passkey-request"
const QRChannelEventPasskeyResponse = "passkey-confirmation"

// Possible final items in the QR channel. In addition to these, an `error` event may be emitted,
// in which case the Error field will have the error that occurred during pairing.
var (
	// QRChannelSuccess is emitted from GetQRChannel when the pairing is successful.
	QRChannelSuccess = QRChannelItem{Event: "success"}
	// QRChannelTimeout is emitted from GetQRChannel if the socket gets disconnected by the server before the pairing is successful.
	QRChannelTimeout = QRChannelItem{Event: "timeout"}
	// QRChannelErrUnexpectedEvent is emitted from GetQRChannel if an unexpected connection event is received,
	// as that likely means that the pairing has already happened before the channel was set up.
	QRChannelErrUnexpectedEvent = QRChannelItem{Event: "err-unexpected-state"}
	// QRChannelClientOutdated is emitted from GetQRChannel if events.ClientOutdated is received.
	QRChannelClientOutdated = QRChannelItem{Event: "err-client-outdated"}
	// QRChannelScannedWithoutMultidevice is emitted from GetQRChannel if events.QRScannedWithoutMultidevice is received.
	QRChannelScannedWithoutMultidevice = QRChannelItem{Event: "err-scanned-without-multidevice"}
)

type qrChannel struct {
	sync.Mutex
	cli       *Client
	log       waLog.Logger
	ctx       context.Context
	handlerID uint32
	closed    atomic.Bool
	output    chan<- QRChannelItem
	stopQRs   chan struct{}

	// WZAPI-PATCH(6): stopQRs used to be closed from exactly one place, so a
	// bare close() was safe. The passkey branch below closes it too, and a
	// second close panics, so both paths now go through stopEmittingQRs.
	stopQRsOnce sync.Once
}

func (qrc *qrChannel) close() bool {
	return qrc.closed.Swap(true) == false
}

// stopEmittingQRs halts the QR rotation goroutine without closing the output
// channel and without disconnecting the client. It is idempotent.
//
// WZAPI-PATCH(6): added for the passkey flow. emitQRs treats <-stopQRs as a
// plain return, unlike every other exit path in it, which closes the channel,
// removes the event handler and calls Disconnect. That distinction is what
// makes this the right lever: pausing the rotation is not the same as ending
// the pairing.
func (qrc *qrChannel) stopEmittingQRs() {
	qrc.stopQRsOnce.Do(func() {
		close(qrc.stopQRs)
	})
}

func (qrc *qrChannel) emitQRs(codes []string) {
	var nextCode string
	for {
		if len(codes) == 0 {
			if qrc.close() {
				qrc.log.Debugf("Ran out of QR codes, closing channel with status %s and disconnecting client", QRChannelTimeout)
				qrc.output <- QRChannelTimeout
				close(qrc.output)
				go qrc.cli.RemoveEventHandler(qrc.handlerID)
				qrc.cli.Disconnect()
			} else {
				qrc.log.Debugf("Ran out of QR codes, but channel is already closed")
			}
			return
		} else if qrc.closed.Load() {
			qrc.log.Debugf("QR code channel is closed, exiting QR emitter")
			return
		}
		timeout := 20 * time.Second
		if len(codes) == 6 {
			timeout = 60 * time.Second
		}
		nextCode, codes = codes[0], codes[1:]
		qrc.log.Debugf("Emitting QR code %s", nextCode)
		select {
		case qrc.output <- QRChannelItem{Code: nextCode, Timeout: timeout, Event: QRChannelEventCode}:
		default:
			qrc.log.Debugf("Output channel didn't accept code, exiting QR emitter")
			if qrc.close() {
				close(qrc.output)
				go qrc.cli.RemoveEventHandler(qrc.handlerID)
				qrc.cli.Disconnect()
			}
			return
		}
		select {
		case <-time.After(timeout):
		case <-qrc.stopQRs:
			qrc.log.Debugf("Got signal to stop QR emitter")
			return
		case <-qrc.cli.expectedDisconnect.GetChan():
			qrc.log.Debugf("Client is expected to disconnect, stopping QR emitter")
			return
		case <-qrc.ctx.Done():
			qrc.log.Debugf("Context is done, stopping QR emitter")
			if qrc.close() {
				close(qrc.output)
				go qrc.cli.RemoveEventHandler(qrc.handlerID)
				qrc.cli.Disconnect()
			}
		}
	}
}

func (qrc *qrChannel) handleEvent(rawEvt any) {
	if qrc.closed.Load() {
		qrc.log.Debugf("Dropping event of type %T, channel is closed", rawEvt)
		return
	}
	var outputType QRChannelItem
	switch evt := rawEvt.(type) {
	case *events.QR:
		qrc.log.Debugf("Received QR code event, starting to emit codes to channel")
		go qrc.emitQRs(slices.Clone(evt.Codes))
		return
	case *events.QRScannedWithoutMultidevice:
		qrc.log.Debugf("QR code scanned without multidevice enabled")
		qrc.output <- QRChannelScannedWithoutMultidevice
		return
	case *events.PairPasskeyRequest:
		// WZAPI-PATCH(6): stop rotating QR codes for the rest of the pairing.
		//
		// A passkey request only arrives once the QR has already been scanned,
		// so the remaining codes are dead weight — but they are not harmless.
		// emitQRs keeps counting down and, when it runs out (60s + 20s x 5 by
		// default), closes the output channel and calls cli.Disconnect(). The
		// WebAuthn ceremony needs a human to go to a WhatsApp-origin tab and
		// authenticate, which routinely takes longer than that, so without this
		// the socket dies mid-ceremony and the pairing fails with no diagnostic.
		//
		// The same goroutine is also the only thing watching qrc.ctx, so
		// stopping it here additionally means the caller's context deadline no
		// longer tears the channel down. Callers must therefore impose their
		// own ceremony timeout; see the note on GetQRChannel.
		qrc.stopEmittingQRs()
		qrc.output <- QRChannelItem{
			Event:          QRChannelEventPasskeyRequest,
			PasskeyRequest: evt,
		}
		return
	case *events.PairPasskeyConfirmation:
		if evt.SkipHandoffUX {
			qrc.log.Debugf("Sending automatic passkey confirmation")
			err := qrc.cli.SendPasskeyConfirmation(qrc.ctx)
			if err != nil {
				qrc.output <- QRChannelItem{
					Event: QRChannelEventError,
					Error: fmt.Errorf("failed to send passkey confirmation automatically: %w", err),
				}
			}
		} else {
			qrc.output <- QRChannelItem{
				Event:               QRChannelEventPasskeyResponse,
				PasskeyConfirmation: evt,
			}
		}
		return
	case *events.PairPasskeyError:
		qrc.output <- QRChannelItem{
			Event: QRChannelEventError,
			Error: evt.Error,
		}
		return
	case *events.ClientOutdated:
		outputType = QRChannelClientOutdated
	case *events.PairSuccess:
		outputType = QRChannelSuccess
	case *events.PairError:
		outputType = QRChannelItem{
			Event: QRChannelEventError,
			Error: evt.Error,
		}
	case *events.Disconnected:
		outputType = QRChannelTimeout
	case *events.Connected, *events.ConnectFailure, *events.LoggedOut, *events.TemporaryBan:
		outputType = QRChannelErrUnexpectedEvent
	default:
		return
	}
	// WZAPI-PATCH(6): this used to close the stopQRs channel directly. The
	// passkey branch may already have closed it, and closing a closed channel
	// panics, so both paths go through the sync.Once now.
	qrc.stopEmittingQRs()
	if qrc.close() {
		qrc.log.Debugf("Closing channel with status %+v", outputType)
		qrc.output <- outputType
		close(qrc.output)
	} else {
		qrc.log.Debugf("Got status %+v, but channel is already closed", outputType)
	}
	// Has to be done in background because otherwise there's a deadlock with eventHandlersLock
	go qrc.cli.RemoveEventHandler(qrc.handlerID)
}

// GetQRChannel returns a channel that automatically outputs a new QR code when the previous one expires.
//
// This must be called *before* Connect(). It will then listen to all the relevant events from the client.
//
// The last value to be emitted will be a special event like "success", "timeout" or another error code
// depending on the result of the pairing. The channel will be closed immediately after one of those.
//
// WZAPI-PATCH(6): the ctx passed here must outlive the whole pairing, including
// a possible WebAuthn ceremony. Two reasons. First, once a passkey-request is
// emitted the QR emitter stops, and it was the only goroutine watching this
// ctx, so its deadline stops being enforced — the caller owns the timeout from
// that point on. Second, this same ctx is what the automatic
// SendPasskeyConfirmation below is issued with, so a short deadline makes the
// auto-confirmation fail exactly when it is needed.
func (cli *Client) GetQRChannel(ctx context.Context) (<-chan QRChannelItem, error) {
	if cli == nil {
		return nil, ErrClientIsNil
	} else if cli.IsConnected() {
		return nil, ErrQRAlreadyConnected
	} else if cli.Store.ID != nil {
		return nil, ErrQRStoreContainsID
	}
	ch := make(chan QRChannelItem, 8)
	qrc := qrChannel{
		output:  ch,
		stopQRs: make(chan struct{}),
		cli:     cli,
		log:     cli.Log.Sub("QRChannel"),
		ctx:     ctx,
	}
	qrc.handlerID = cli.AddEventHandler(qrc.handleEvent)
	return ch, nil
}
