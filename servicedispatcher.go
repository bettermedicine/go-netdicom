package netdicom

import (
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/bettermedicine/go-netdicom/dimse"
	"github.com/grailbio/go-dicom/dicomlog"
)

// serviceDispatcher multiplexes statemachine upcall events to DIMSE commands.
type serviceDispatcher struct {
	label      string          // for logging.
	downcallCh chan stateEvent // for sending PDUs to the statemachine.

	mu sync.Mutex

	// Set of active DIMSE commands running. Keys are message IDs.
	activeCommands map[dimse.MessageID]*serviceCommandState // guarded by mu

	// A callback to be called when a dimse request message arrives. Keys
	// are DIMSE CommandField. The callback typically creates a new command
	// by calling findOrCreateCommand.
	callbacks map[int]serviceCallback // guarded by mu

	// The last message ID used in newCommand(). Used to avoid creating duplicate
	// IDs.
	lastMessageID dimse.MessageID

	// AE titles from the association handshake. Set once and reused for all commands.
	// guarded by mu
	CalledAETitle  string
	CallingAETitle string
}

type serviceCallback func(msg dimse.Message, data []byte, cs *serviceCommandState)

// Per-DIMSE-command state.
type serviceCommandState struct {
	disp      *serviceDispatcher  // Parent.
	messageID dimse.MessageID     // Command's MessageID.
	context   contextManagerEntry // Transfersyntax/sopclass for this command.
	cm        *contextManager     // For looking up context -> transfersyntax/sopclass mappings

	// upcallCh streams command+data for this messageID.
	upcallCh chan upcallEvent

	// AE titles from the association handshake.
	CalledAETitle  string
	CallingAETitle string
}

// Send a command+data combo to the remote peer. data may be nil.
func (cs *serviceCommandState) sendMessage(cmd dimse.Message, data []byte) {
	if s := cmd.GetStatus(); s != nil && s.Status != dimse.StatusSuccess && s.Status != dimse.StatusPending {
		dicomlog.Vprintf(0, "dicom.serviceDispatcher(%s): Sending DIMSE error: %v %v", cs.disp.label, cmd, cs.disp)
	} else {
		dicomlog.Vprintf(1, "dicom.serviceDispatcher(%s): Sending DIMSE message: %v %v", cs.disp.label, cmd, cs.disp)
	}
	payload := &stateEventDIMSEPayload{
		abstractSyntaxName: cs.context.abstractSyntaxUID,
		command:            cmd,
		data:               data,
	}
	cs.disp.downcallCh <- stateEvent{
		event:        evt09,
		pdu:          nil,
		conn:         nil,
		dimsePayload: payload,
	}
}

func (disp *serviceDispatcher) findOrCreateCommand(
	msgID dimse.MessageID,
	cm *contextManager,
	context contextManagerEntry) (*serviceCommandState, bool) {
	disp.mu.Lock()
	defer disp.mu.Unlock()
	if cs, ok := disp.activeCommands[msgID]; ok {
		return cs, true
	}
	cs := &serviceCommandState{
		disp:           disp,
		messageID:      msgID,
		cm:             cm,
		context:        context,
		upcallCh:       make(chan upcallEvent, 128),
		CalledAETitle:  disp.CalledAETitle,
		CallingAETitle: disp.CallingAETitle,
	}
	disp.activeCommands[msgID] = cs
	dicomlog.Vprintf(1, "dicom.serviceDispatcher(%s): Start command %+v", disp.label, cs)
	return cs, false
}

// Create a new serviceCommandState with an unused message ID.  Returns an error
// if it fails to allocate a message ID.
func (disp *serviceDispatcher) newCommand(
	cm *contextManager, context contextManagerEntry) (*serviceCommandState, error) {
	disp.mu.Lock()
	defer disp.mu.Unlock()

	for msgID := disp.lastMessageID + 1; msgID != disp.lastMessageID; msgID++ {
		if _, ok := disp.activeCommands[msgID]; ok {
			continue
		}

		cs := &serviceCommandState{
			disp:           disp,
			messageID:      msgID,
			cm:             cm,
			context:        context,
			upcallCh:       make(chan upcallEvent, 128),
			CalledAETitle:  disp.CalledAETitle,
			CallingAETitle: disp.CallingAETitle,
		}
		disp.activeCommands[msgID] = cs
		disp.lastMessageID = msgID
		dicomlog.Vprintf(1, "dicom.serviceDispatcher: Start new command %+v", cs)
		return cs, nil
	}
	return nil, fmt.Errorf("Failed to allocate a message ID (too many outstading?)")
}

func (disp *serviceDispatcher) deleteCommand(cs *serviceCommandState) {
	disp.mu.Lock()
	dicomlog.Vprintf(1, "dicom.serviceDispatcher(%s): Finish provider command %v", disp.label, cs.messageID)
	if _, ok := disp.activeCommands[cs.messageID]; !ok {
		panic(fmt.Sprintf("cs %+v", cs))
	}
	delete(disp.activeCommands, cs.messageID)
	disp.mu.Unlock()
}

func (disp *serviceDispatcher) registerCallback(commandField int, cb serviceCallback) {
	disp.mu.Lock()
	disp.callbacks[commandField] = cb
	disp.mu.Unlock()
}

func (disp *serviceDispatcher) unregisterCallback(commandField int) {
	disp.mu.Lock()
	delete(disp.callbacks, commandField)
	disp.mu.Unlock()
}

func (disp *serviceDispatcher) handleEvent(event upcallEvent) {
	if event.eventType == upcallEventHandshakeCompleted {
		// Store AE titles in dispatcher for use in all commands on this association
		disp.mu.Lock()
		// these are 16 bytes wide and space padded by default
		// so lets trim spaces
		disp.CalledAETitle = strings.TrimSpace(event.CalledAETitle)
		disp.CallingAETitle = strings.TrimSpace(event.CallingAETitle)
		// Update all existing commands with the AE titles
		for _, cs := range disp.activeCommands {
			cs.CalledAETitle = event.CalledAETitle
			cs.CallingAETitle = event.CallingAETitle
		}
		disp.mu.Unlock()
		return
	}
	doassert(event.eventType == upcallEventData)
	doassert(event.command != nil)
	context, err := event.cm.lookupByContextID(event.contextID)
	if err != nil {
		dicomlog.Vprintf(0, "dicom.serviceDispatcher(%s): Invalid context ID %d: %v", disp.label, event.contextID, err)
		disp.downcallCh <- stateEvent{event: evt19, pdu: nil, err: err}
		return
	}
	messageID := event.command.GetMessageID()
	dc, found := disp.findOrCreateCommand(messageID, event.cm, context)
	if found {
		dicomlog.Vprintf(1, "dicom.serviceDispatcher(%s): Forwarding command to existing command: %+v %+v", disp.label, event.command, dc)
		dc.upcallCh <- event
		dicomlog.Vprintf(1, "dicom.serviceDispatcher(%s): Done forwarding command to existing command: %+v %+v", disp.label, event.command, dc)
		return
	}
	disp.mu.Lock()
	cb := disp.callbacks[event.command.CommandField()]
	// Get AE titles from dispatcher (set during handshake)
	calledAE := disp.CalledAETitle
	callingAE := disp.CallingAETitle
	disp.mu.Unlock()
	go func() {
		// Update AE titles on the command state from dispatcher
		dc.CalledAETitle = calledAE
		dc.CallingAETitle = callingAE
		cb(event.command, event.data, dc)
		disp.deleteCommand(dc)
	}()
}

// Must be called exactly once to shut down the dispatcher.
func (disp *serviceDispatcher) close() {
	disp.mu.Lock()
	for _, cs := range disp.activeCommands {
		close(cs.upcallCh)
	}
	disp.mu.Unlock()
	// TODO(saito): prevent new command from launching.
}

func newServiceDispatcher(label string) *serviceDispatcher {
	return &serviceDispatcher{
		label:          label,
		downcallCh:     make(chan stateEvent, 128),
		activeCommands: make(map[dimse.MessageID]*serviceCommandState),
		callbacks:      make(map[int]serviceCallback),
		// We're initializing it at a LARGE size because otherwise we can get message ID conflicts in the callback map.
		// TODO: segregate client-initiated and server-initiated message IDs and their callbacks into separate id spaces.
		lastMessageID: math.MaxUint16 - (4 * 1024),
	}
}
