package persistent

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"

	"golang.org/x/sync/errgroup"
)

const (
	PersistentUndefinedPrefix = byte(iota)
	PersistentBoolPrefix

	PersistentInt8Prefix
	PersistentInt16Prefix
	PersistentInt32Prefix
	PersistentInt64Prefix

	PersistentUint8Prefix
	PersistentUint16Prefix
	PersistentUint32Prefix
	PersistentUint64Prefix

	PersistentFloat32Prefix
	PersistentFloat64Prefix

	PersistentStringPrefix
	PersistentSlicePrefix

	PersistentBytePrefix
)

type Storager interface {
	Save([]byte, []byte) error
	Load([]byte) ([]byte, error)
}

type Persistent interface {
	Persist(Storager, []byte) error
	Restore(Storager, []byte) error
}

func EmptyPersistentBool() *PersistentBool {
	p := new(PersistentBool)
	return p
}

func NewPersistentBool(v bool) *PersistentBool {
	p := new(PersistentBool)
	*p = PersistentBool(v)
	return p
}

type PersistentBool bool

func (p *PersistentBool) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentBoolPrefix}, k...)

	if *p {
		return s.Save(key, []byte{1})
	} else {
		return s.Save(key, []byte{0})
	}
}

func (p *PersistentBool) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentBoolPrefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	*p = dt[0] == byte(1)
	return nil
}

func EmptyPersistentInt8() *PersistentInt8 {
	p := new(PersistentInt8)
	return p
}

func NewPersistentInt8(v int8) *PersistentInt8 {
	p := new(PersistentInt8)
	*p = PersistentInt8(v)
	return p
}

type PersistentInt8 int8

func (p *PersistentInt8) Persist(s Storager, key []byte) error {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, int8(*p))
	if err != nil {
		return err
	}

	return s.Save(key, buff.Bytes())
}

func (p *PersistentInt8) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentInt8Prefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(dt)
	return binary.Read(buff, binary.LittleEndian, p)
}

func EmptyPersistentInt16() *PersistentInt16 {
	p := new(PersistentInt16)
	return p
}

func NewPersistentInt16(v int16) *PersistentInt16 {
	p := new(PersistentInt16)
	*p = PersistentInt16(v)
	return p
}

type PersistentInt16 int16

func (p *PersistentInt16) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentInt16Prefix}, k...)

	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, int16(*p))
	if err != nil {
		return err
	}

	return s.Save(key, buff.Bytes())
}

func (p *PersistentInt16) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentInt16Prefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(dt)
	return binary.Read(buff, binary.LittleEndian, p)
}

func EmptyPersistentInt32() *PersistentInt32 {
	p := new(PersistentInt32)
	return p
}

func NewPersistentInt32(v int32) *PersistentInt32 {
	p := new(PersistentInt32)
	*p = PersistentInt32(v)
	return p
}

type PersistentInt32 int32

func (p *PersistentInt32) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentInt32Prefix}, k...)

	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, int32(*p))
	if err != nil {
		return err
	}

	return s.Save(key, buff.Bytes())
}

func (p *PersistentInt32) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentInt32Prefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(dt)
	return binary.Read(buff, binary.LittleEndian, p)
}

func EmptyPersistentInt64() *PersistentInt64 {
	p := new(PersistentInt64)
	return p
}

func NewPersistentInt64(v int64) *PersistentInt64 {
	p := new(PersistentInt64)
	*p = PersistentInt64(v)
	return p
}

type PersistentInt64 int64

func (p *PersistentInt64) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentInt64Prefix}, k...)

	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, int64(*p))
	if err != nil {
		return err
	}

	return s.Save(key, buff.Bytes())
}

func (p *PersistentInt64) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentInt64Prefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(dt)
	return binary.Read(buff, binary.LittleEndian, p)
}

func EmptyPersistentUint8() *PersistentUint8 {
	p := new(PersistentUint8)
	return p
}

func NewPersistentUint8(v uint8) *PersistentUint8 {
	p := new(PersistentUint8)
	*p = PersistentUint8(v)
	return p
}

type PersistentUint8 uint8

func (p *PersistentUint8) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentUint8Prefix}, k...)

	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, uint8(*p))
	if err != nil {
		return err
	}

	return s.Save(key, buff.Bytes())
}

func (p *PersistentUint8) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentUint8Prefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(dt)
	return binary.Read(buff, binary.LittleEndian, p)
}

func EmptyPersistentUint16() *PersistentUint16 {
	p := new(PersistentUint16)
	return p
}

func NewPersistentUint16(v uint16) *PersistentUint16 {
	p := new(PersistentUint16)
	*p = PersistentUint16(v)
	return p
}

type PersistentUint16 uint16

func (p *PersistentUint16) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentUint16Prefix}, k...)

	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, uint16(*p))
	if err != nil {
		return err
	}

	return s.Save(key, buff.Bytes())
}

func (p *PersistentUint16) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentUint16Prefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(dt)
	return binary.Read(buff, binary.LittleEndian, p)
}

func EmptyPersistentUint32() *PersistentUint32 {
	p := new(PersistentUint32)
	return p
}

func NewPersistentUint32(v uint32) *PersistentUint32 {
	p := new(PersistentUint32)
	*p = PersistentUint32(v)
	return p
}

type PersistentUint32 uint32

func (p *PersistentUint32) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentUint32Prefix}, k...)

	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, uint32(*p))
	if err != nil {
		return err
	}

	return s.Save(key, buff.Bytes())
}

func (p *PersistentUint32) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentUint32Prefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(dt)
	return binary.Read(buff, binary.LittleEndian, p)
}

func EmptyPersistentUint64() *PersistentUint64 {
	p := new(PersistentUint64)
	return p
}

func NewPersistentUint64(v uint64) *PersistentUint64 {
	p := new(PersistentUint64)
	*p = PersistentUint64(v)
	return p
}

type PersistentUint64 uint64

func (p *PersistentUint64) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentUint64Prefix}, k...)

	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, uint64(*p))
	if err != nil {
		return err
	}

	return s.Save(key, buff.Bytes())
}

func (p *PersistentUint64) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentUint64Prefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(dt)
	return binary.Read(buff, binary.LittleEndian, p)
}

func EmptyPersistentFloat32() *PersistentFloat32 {
	p := new(PersistentFloat32)
	return p
}

func NewPersistentFloat32(v float32) *PersistentFloat32 {
	p := new(PersistentFloat32)
	*p = PersistentFloat32(v)
	return p
}

type PersistentFloat32 float32

func (p *PersistentFloat32) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentFloat32Prefix}, k...)

	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, float32(*p))
	if err != nil {
		return err
	}

	return s.Save(key, buff.Bytes())
}

func (p *PersistentFloat32) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentFloat32Prefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(dt)
	return binary.Read(buff, binary.LittleEndian, p)
}

func EmptyPersistentFloat64() *PersistentFloat64 {
	p := new(PersistentFloat64)
	return p
}

func NewPersistentFloat64(v float64) *PersistentFloat64 {
	p := new(PersistentFloat64)
	*p = PersistentFloat64(v)
	return p
}

type PersistentFloat64 float64

func (p *PersistentFloat64) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentFloat64Prefix}, k...)

	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.LittleEndian, float64(*p))
	if err != nil {
		return err
	}

	return s.Save(key, buff.Bytes())
}

func (p *PersistentFloat64) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentFloat64Prefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	buff := bytes.NewBuffer(dt)
	return binary.Read(buff, binary.LittleEndian, p)
}

func EmptyPersistentByte() *PersistentByte {
	p := new(PersistentByte)
	return p
}

func NewPersistentByte(v byte) *PersistentByte {
	p := new(PersistentByte)
	*p = PersistentByte(v)
	return p
}

type PersistentByte byte

func (p *PersistentByte) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentBytePrefix}, k...)

	return s.Save(key, []byte{byte(*p)})
}

func (p *PersistentByte) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentBytePrefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	if len(dt) != 1 {
		return fmt.Errorf("%s is not a byte", key)
	}

	*p = PersistentByte(dt[0])
	return nil
}

func EmptyPersistentString() *PersistentString {
	p := new(PersistentString)
	return p
}

func NewPersistentString(v string) *PersistentString {
	p := new(PersistentString)
	*p = PersistentString(v)
	return p
}

type PersistentString string

func (p *PersistentString) Persist(s Storager, k []byte) error {
	key := append([]byte{PersistentStringPrefix}, k...)
	return s.Save(key, []byte(*p))
}

func (p *PersistentString) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentStringPrefix}, k...)

	dt, err := s.Load([]byte(key))
	if err != nil {
		return err
	}

	*p = PersistentString(dt)
	return nil
}

type PersistentSlice []Persistent

func (ps *PersistentSlice) Persist(s Storager, k []byte) error {
	var iPrefix byte = PersistentUndefinedPrefix

	if len(*ps) > 0 {
		switch []Persistent(*ps)[0].(type) {
		default:
			iPrefix = PersistentUndefinedPrefix
		case *PersistentBool:
			iPrefix = PersistentBoolPrefix
		case *PersistentInt8:
			iPrefix = PersistentInt8Prefix
		case *PersistentInt16:
			iPrefix = PersistentInt16Prefix
		case *PersistentInt32:
			iPrefix = PersistentInt32Prefix
		case *PersistentInt64:
			iPrefix = PersistentInt64Prefix
		case *PersistentUint8:
			iPrefix = PersistentUint8Prefix
		case *PersistentUint16:
			iPrefix = PersistentUint16Prefix
		case *PersistentUint32:
			iPrefix = PersistentUint32Prefix
		case *PersistentUint64:
			iPrefix = PersistentUint64Prefix
		case *PersistentFloat32:
			iPrefix = PersistentFloat32Prefix
		case *PersistentFloat64:
			iPrefix = PersistentFloat64Prefix
		case *PersistentString:
			iPrefix = PersistentStringPrefix
		case *PersistentByte:
			iPrefix = PersistentBytePrefix
		}
	}

	key := append([]byte{PersistentSlicePrefix}, k...)
	g := new(errgroup.Group)

	lock := new(sync.Mutex)
	i := 0

	for range *ps {
		g.Go(func() error {
			var index int
			lock.Lock()
			index = i
			p := (*ps)[index]
			i++
			lock.Unlock()

			k := []byte(fmt.Sprintf("%s:%c%d", key, iPrefix, index))
			return p.Persist(s, k)
		})
	}

	length := PersistentUint64(len(*ps))
	err := length.Persist(s, append([]byte("l"), key...))
	if err != nil {
		return err
	}

	prefix := PersistentByte(iPrefix)
	err = prefix.Persist(s, append([]byte("t"), key...))
	if err != nil {
		return err
	}

	return g.Wait()
}

func (ps *PersistentSlice) Restore(s Storager, k []byte) error {
	key := append([]byte{PersistentSlicePrefix}, k...)
	length := new(PersistentUint64)
	err := length.Restore(s, append([]byte("l"), key...))
	if err != nil {
		return err
	}

	prefix := new(PersistentByte)
	err = prefix.Restore(s, append([]byte("t"), key...))
	if err != nil {
		return err
	}

	slc := make([]Persistent, *length)

	g := new(errgroup.Group)

	lock := new(sync.Mutex)
	i := 0

	for range slc {
		g.Go(func() error {
			var index int
			lock.Lock()
			index = i
			i++
			lock.Unlock()

			iK := []byte(fmt.Sprintf("%s:%c%d", key, *prefix, index))

			switch byte(*prefix) {
			case PersistentBoolPrefix:
				slc[index] = new(PersistentBool)
			case PersistentInt8Prefix:
				slc[index] = new(PersistentInt8)
			case PersistentInt16Prefix:
				slc[index] = new(PersistentInt16)
			case PersistentInt32Prefix:
				slc[index] = new(PersistentInt32)
			case PersistentInt64Prefix:
				slc[index] = new(PersistentInt64)
			case PersistentUint8Prefix:
				slc[index] = new(PersistentUint8)
			case PersistentUint16Prefix:
				slc[index] = new(PersistentUint16)
			case PersistentUint32Prefix:
				slc[index] = new(PersistentUint32)
			case PersistentUint64Prefix:
				slc[index] = new(PersistentUint64)
			case PersistentFloat32Prefix:
				slc[index] = new(PersistentFloat32)
			case PersistentFloat64Prefix:
				slc[index] = new(PersistentFloat64)
			case PersistentStringPrefix:
				slc[index] = new(PersistentString)
			case PersistentSlicePrefix:
				slc[index] = new(PersistentSlice)
			case PersistentBytePrefix:
				slc[index] = new(PersistentByte)
			}

			return slc[index].Restore(s, iK)
		})
	}

	err = g.Wait()
	if err != nil {
		return err
	}

	*ps = slc
	return nil
}

func PersistStruct(id string, dt interface{}, s Storager, errHandler func(error)) {
	dtType := reflect.TypeOf(dt).Elem()
	dtValue := reflect.ValueOf(dt).Elem()

	pType := reflect.TypeOf((*Persistent)(nil)).Elem()

	for i := 0; i < dtType.NumField(); i++ {
		if dtType.Field(i).Type.Implements(pType) {
			if dtValue.Field(i).IsNil() {
				continue
			}

			cRet := dtValue.Field(i).MethodByName("Persist").Call([]reflect.Value{
				reflect.ValueOf(s),
				reflect.ValueOf([]byte(fmt.Sprintf("%s/%s", id, dtType.Field(i).Name))),
			})

			reflect.ValueOf(errHandler).Call(cRet)
		}
	}
}

func RestoreStruct(id string, dt interface{}, s Storager, errHandler func(error)) {
	dtType := reflect.TypeOf(dt).Elem()
	dtValue := reflect.ValueOf(dt).Elem()

	pType := reflect.TypeOf((*Persistent)(nil)).Elem()

	for i := 0; i < dtType.NumField(); i++ {
		if dtType.Field(i).Type.Implements(pType) {
			if dtValue.Field(i).IsNil() {
				dtValue.Field(i).Set(reflect.New(dtType.Field(i).Type.Elem()))
			}

			cRet := dtValue.Field(i).MethodByName("Restore").Call([]reflect.Value{
				reflect.ValueOf(s),
				reflect.ValueOf([]byte(fmt.Sprintf("%s/%s", id, dtType.Field(i).Name))),
			})

			reflect.ValueOf(errHandler).Call(cRet)
		}
	}
}
