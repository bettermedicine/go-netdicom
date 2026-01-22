package netdicom_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"log"

	"github.com/bettermedicine/go-netdicom"
	"github.com/bettermedicine/go-netdicom/dimse"
	"github.com/bettermedicine/go-netdicom/sopclass"
	"github.com/grailbio/go-dicom"
	"github.com/grailbio/go-dicom/dicomio"
	"github.com/grailbio/go-dicom/dicomlog"
	"github.com/grailbio/go-dicom/dicomtag"
	"github.com/stretchr/testify/assert"
)

const serverPort = 10400
const FooClientAETitle = "FOO_CLIENT"
const FooClientCalledAETitle = "TEST_SCP_FOO"
const BarClientAETitle = "BAR_CLIENT"
const BarClientCalledAETitle = "TEST_SCP_BAR"

// the payload we'll be sending.
// we'll be placing an unique identifier in the
// patient name field so we can discern whether or not the correct
// ae titles were used.
type AETitleParsingTestCase struct {
	UniquePatientName string
	CallingAETitle    string
	CalledAETitle     string
}

func TestCStore(t *testing.T) {
	dicomlog.SetLevel(-1)
	resultsCh := make(chan *AETitleParsingTestCase, 4)
	defer close(resultsCh)

	// set up a dicom server
	serverParams := netdicom.ServiceProviderParams{
		AETitle: "TEST_SCP",
		CStore: func(connState netdicom.ConnectionState, transferSyntaxUID string,
			sopClassUID string,
			sopInstanceUID string,
			data []byte) dimse.Status {
			// we parse the file to extract the patient name
			buffer := bytes.Buffer{}
			enc := dicomio.NewEncoderWithTransferSyntax(&buffer, transferSyntaxUID)
			dicom.WriteFileHeader(enc,
				[]*dicom.Element{
					dicom.MustNewElement(dicomtag.TransferSyntaxUID, transferSyntaxUID),
					dicom.MustNewElement(dicomtag.MediaStorageSOPClassUID, sopClassUID),
					dicom.MustNewElement(dicomtag.MediaStorageSOPInstanceUID, sopInstanceUID),
				})
			enc.WriteBytes(data)

			ds, err := dicom.ReadDataSet(&buffer, dicom.ReadOptions{})
			if err != nil {
				t.Fatalf("Failed to read dataset in C-STORE handler: %v", err)
			}

			patientID, err := ds.FindElementByTag(dicomtag.PatientID)
			if err != nil {
				t.Fatalf("Failed to find PatientID element: %v", err)
			}
			log.Printf("Remote address %s", connState.RemoteAddress.String())
			result := &AETitleParsingTestCase{
				UniquePatientName: patientID.Value[0].(string),
				CalledAETitle:     connState.CalledAETitle,
				CallingAETitle:    connState.CallingAETitle,
			}
			log.Printf("handled name %s", result.UniquePatientName)
			resultsCh <- result
			return dimse.Status{Status: dimse.StatusSuccess}
		},
	}

	sp, err := netdicom.NewServiceProvider(serverParams, fmt.Sprintf(":%d", serverPort))
	if err != nil {
		t.Fatalf("Failed to create service provider: %v", err)
	}
	go func() {
		sp.Run()
		log.Println("DICOM server started")
	}()
	// these will be sent from the same SU.
	fooClientCases := []AETitleParsingTestCase{
		{
			UniquePatientName: "FOO0001",
			CallingAETitle:    FooClientAETitle,
			CalledAETitle:     FooClientCalledAETitle,
		},
		{
			UniquePatientName: "FOO0002",
			CallingAETitle:    FooClientAETitle,
			CalledAETitle:     FooClientCalledAETitle,
		},
	}
	// these will be separate SU-s.
	barClientCases := []AETitleParsingTestCase{
		{
			UniquePatientName: "BAR0001",
			CallingAETitle:    BarClientAETitle,
			CalledAETitle:     BarClientCalledAETitle,
		},
		{
			UniquePatientName: "BAR0002",
			CallingAETitle:    BarClientAETitle,
			CalledAETitle:     BarClientCalledAETitle,
		},
	}
	// we add both foo and bar client cases
	// into a map for easy lookup later.
	expectedReceives := map[string]AETitleParsingTestCase{}
	for _, tc := range fooClientCases {
		expectedReceives[tc.UniquePatientName] = tc
	}
	for _, tc := range barClientCases {
		expectedReceives[tc.UniquePatientName] = tc
	}

	// lets handle foo first.
	suParams := netdicom.ServiceUserParams{
		CalledAETitle:  FooClientCalledAETitle,
		CallingAETitle: FooClientAETitle,
		SOPClasses:     sopclass.StorageClasses,
	}
	su, err := netdicom.NewServiceUser(suParams)
	if err != nil {
		t.Fatalf("Failed to create service user: %v", err)
	}

	su.Connect(fmt.Sprintf("localhost:%d", serverPort))
	for _, tc := range fooClientCases {
		// load fresh dataset from disk
		ds, err := dicom.ReadDataSetFromFile("./testdata/IM-0001-0003.dcm", dicom.ReadOptions{})
		if err != nil {
			t.Fatalf("Failed to read DICOM file: %v", err)
		}
		// patch in unique patient name
		elem, err := ds.FindElementByTag(dicomtag.PatientID)
		if err != nil {
			t.Fatalf("Failed to find PatientID element: %v", err)
		}
		elem.Value = []interface{}{tc.UniquePatientName}
		err = su.CStore(ds)
		if err != nil {
			t.Fatalf("C-STORE failed for case %s: %v", tc.UniquePatientName, err)
		}
	}

	// now for bar client cases - each in their own SU.
	for _, tc := range barClientCases {
		suParams := netdicom.ServiceUserParams{
			CalledAETitle:  tc.CalledAETitle,
			CallingAETitle: tc.CallingAETitle,
			SOPClasses:     sopclass.StorageClasses,
		}
		su, err := netdicom.NewServiceUser(suParams)
		if err != nil {
			t.Fatalf("Failed to create service user: %v", err)
		}

		su.Connect(fmt.Sprintf("localhost:%d", serverPort))
		// load fresh dataset from disk
		ds, err := dicom.ReadDataSetFromFile("./testdata/IM-0001-0003.dcm", dicom.ReadOptions{})
		if err != nil {
			t.Fatalf("Failed to read DICOM file: %v", err)
		}
		// patch in unique patient name
		elem, err := ds.FindElementByTag(dicomtag.PatientID)
		if err != nil {
			t.Fatalf("Failed to find PatientID element: %v", err)
		}
		elem.Value = []interface{}{tc.UniquePatientName}
		err = su.CStore(ds)
		if err != nil {
			t.Fatalf("C-STORE failed for case %s: %v", tc.UniquePatientName, err)
		}
	}

	for {
		select {
		case <-time.After(5 * time.Second):
			t.Fatalf("Service provider failed to start in time")
		case result, ok := <-resultsCh:
			if !ok {
				t.Fatalf("results channel closed early")
			}
			// we are in a single for loop so we don't need to guard
			// expectedReceives with a mutex.
			expected, found := expectedReceives[result.UniquePatientName]
			if !found {
				t.Errorf("Received unexpected patient name: %s", result.UniquePatientName)
			} else {
				// assert ae titles match expected values.
				assert.Equal(t, expected.CallingAETitle, result.CallingAETitle,
					"Calling AE Title mismatch for patient %s", result.UniquePatientName)
				assert.Equal(t, expected.CalledAETitle, result.CalledAETitle,
					"Called AE Title mismatch for patient %s", result.UniquePatientName)
				// remove from map to mark as seen.
				delete(expectedReceives, result.UniquePatientName)
			}
			// if we've seen all expected results, we are done.
			if len(expectedReceives) == 0 {
				log.Printf("All expected C-STORE requests received.")
				return
			}
		}
	}
}
