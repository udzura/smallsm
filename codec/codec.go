package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/udzura/smallsm/log"
)

var (
	nullByte  []byte = []byte{0}
	int32Size int    = binary.Size(int32(1))

	InvalidLogFormat error = errors.New("invalid log format")
)

type Encoder struct {
	w io.Writer
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w}
}

func (e *Encoder) Encode(log *log.Log) error {
	klen := int32(len(log.Key) + 1)
	vlen := int32(len(log.Value) + 1)
	deleted := int8(0)
	if log.Deleted {
		deleted = 1
	}
	buf := make([]byte, 0)
	w := bytes.NewBuffer(buf)

	_ = binary.Write(w, binary.LittleEndian, klen)
	_, _ = w.Write([]byte(log.Key))
	_, _ = w.Write(nullByte)
	_ = binary.Write(w, binary.LittleEndian, vlen)
	_, _ = w.Write([]byte(log.Value))
	_, _ = w.Write(nullByte)
	_ = binary.Write(w, binary.LittleEndian, deleted)
	_, _ = w.Write(nullByte)
	_, _ = w.Write(nullByte)

	if _, err := e.w.Write(w.Bytes()); err != nil {
		return err
	}

	return nil
}

type Decoder struct {
	r io.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r}
}

func (d *Decoder) Decode() (*log.Log, error) {
	var klen, vlen int32
	var deleted int8 = -1
	err := binary.Read(d.r, binary.LittleEndian, &klen)
	if err != nil {
		return nil, err
	}
	key := make([]byte, klen)
	_, err = d.r.Read(key)
	if err != nil {
		return nil, err
	}
	if key[len(key)-1] != '\000' {
		return nil, InvalidLogFormat
	}

	err = binary.Read(d.r, binary.LittleEndian, &vlen)
	if err != nil {
		return nil, err
	}
	value := make([]byte, vlen)
	_, err = d.r.Read(value)
	if err != nil {
		return nil, err
	}

	if value[len(value)-1] != '\000' {
		return nil, InvalidLogFormat
	}

	err = binary.Read(d.r, binary.LittleEndian, &deleted)
	if err != nil {
		return nil, err
	}
	if deleted != 0 && deleted != 1 {
		return nil, InvalidLogFormat
	}

	sentinel := make([]byte, 2)
	_, err = d.r.Read(sentinel)
	if err != nil {
		return nil, err
	}
	if sentinel[0] != '\000' || sentinel[1] != '\000' {
		return nil, InvalidLogFormat
	}

	valueDeleted := false
	if deleted == 1 {
		valueDeleted = true
	}
	log := &log.Log{
		Key:     string(key[0 : len(key)-1]),
		Value:   string(value[0 : len(value)-1]),
		Deleted: valueDeleted,
	}

	return log, nil
}
