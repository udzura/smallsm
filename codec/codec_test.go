package codec

import (
	"os"
	"testing"

	"github.com/udzura/smallsm"
)

func TestEncodeDecode(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "test-XXXXXXXX.log")
	if err != nil {
		t.Fatal(err)
	}

	e := NewEncoder(f)
	log1 := smallsm.SmalLog{
		Key:   "/home/udzura/file.1",
		Value: "My file",
	}
	log2 := smallsm.SmalLog{
		Key:   "/home/udzura/file.2",
		Value: "My file 2",
	}
	log3 := smallsm.SmalLog{
		Key:     "/home/udzura/file.3",
		Value:   "",
		Deleted: true,
	}

	if err := e.Encode(&log1); err != nil {
		t.Errorf("Should not get error: %v", err)
	}
	if err := e.Encode(&log2); err != nil {
		t.Errorf("Should not get error: %v", err)
	}
	if err := e.Encode(&log3); err != nil {
		t.Errorf("Should not get error: %v", err)
	}
	f.Close()
	f2, err := os.Open(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	f2.Seek(0, 0)
	d := NewDecoder(f2)

	log4, err := d.Decode()
	if err != nil {
		t.Errorf("Should not get error: %v", err)
	}
	if log4.Key != "/home/udzura/file.1" {
		t.Errorf("Expect key /home/udzura/file.1, got: %s", log4.Key)
	}
	if log4.Value != "My file" {
		t.Errorf("Expect value `My file`, got: %s", log4.Value)
	}

	log5, err := d.Decode()
	if err != nil {
		t.Errorf("Should not get error: %v", err)
	}
	if log5.Key != "/home/udzura/file.2" {
		t.Errorf("Expect key /home/udzura/file.2, got: %s", log5.Key)
	}
	if log5.Value != "My file 2" {
		t.Errorf("Expect value `My file 2`, got: %s", log5.Value)
	}

	log6, err := d.Decode()
	if err != nil {
		t.Errorf("Should not get error: %v", err)
	}
	if log6.Key != "/home/udzura/file.3" {
		t.Errorf("Expect key /home/udzura/file.3, got: %s", log6.Key)
	}
	if !log6.Deleted {
		t.Errorf("Expect log6 to be deleted")
	}
}
