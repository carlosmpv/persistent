package persistent

import (
	"errors"
	"testing"
)

type testStorager struct {
	db map[string][]byte
}

func (ts *testStorager) Save(k, v []byte) error {
	ts.db[string(k)] = v
	return nil
}

func (ts *testStorager) Load(k []byte) ([]byte, error) {
	val, ok := ts.db[string(k)]
	if !ok {
		return nil, errors.New("value is not stored")
	}

	return val, nil
}

func TestBool(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testBool := PersistentBool(true)
	testBoolKey := []byte("test")

	if err := testBool.Persist(storager, testBoolKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentBool(false)
	if err := nTest.Restore(storager, testBoolKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testBool {
		t.Fail()
	}
}

func TestStr(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testStr := NewPersistentString("Olá mundo!")
	testStrKey := []byte("test")

	if err := testStr.Persist(storager, testStrKey); err != nil {
		t.Fatal(err)
	}

	nTest := EmptyPersistentString()
	if err := nTest.Restore(storager, testStrKey); err != nil {
		t.Fatal(err)
	}

	if *nTest != *testStr {
		t.Fail()
	}
}

func TestInt8(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentInt8(-38)
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentInt8(0)
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testInt {
		t.Fail()
	}
}

func TestInt16(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentInt16(-38)
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentInt16(0)
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testInt {
		t.Fail()
	}
}

func TestInt32(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentInt32(-388)
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentInt32(0)
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testInt {
		t.Fail()
	}
}

func TestInt64(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentInt64(-388)
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentInt64(0)
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testInt {
		t.Fail()
	}
}

func TestUint8(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentUint8(9)
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentUint8(0)
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testInt {
		t.Fail()
	}
}

func TestUint16(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentUint16(88)
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentUint16(0)
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testInt {
		t.Fail()
	}
}
func TestUint32(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentUint32(88)
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentUint32(0)
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testInt {
		t.Fail()
	}
}

func TestUint64(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentUint64(88)
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentUint64(0)
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testInt {
		t.Fail()
	}
}

func TestFloat32(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentFloat32(-88.425)
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentFloat32(0)
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testInt {
		t.Fail()
	}
}

func TestFloat64(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentFloat64(-88.425)
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentFloat64(0)
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	t.Log(nTest)

	if nTest != testInt {
		t.Fail()
	}
}

func TestByte(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testInt := PersistentByte('a')
	testIntKey := []byte("test")

	if err := testInt.Persist(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentByte(byte(0))
	if err := nTest.Restore(storager, testIntKey); err != nil {
		t.Fatal(err)
	}

	if nTest != testInt {
		t.Fail()
	}
}

func TestSlice(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	testSlice := PersistentSlice([]Persistent{
		NewPersistentInt64(-44),
		NewPersistentInt64(33),
		NewPersistentInt64(33),
	})
	testSliceKey := []byte("test")

	if err := testSlice.Persist(storager, testSliceKey); err != nil {
		t.Fatal(err)
	}

	nTest := PersistentSlice([]Persistent{})
	if err := nTest.Restore(storager, testSliceKey); err != nil {
		t.Fatal(err)
	}

	t.Log(int64(*nTest[2].(*PersistentInt64)))
}

func TestPersistStruct(t *testing.T) {
	storager := &testStorager{map[string][]byte{}}

	type pessoa struct {
		Nome    *PersistentString
		Idade   *PersistentUint32
		Altura  *PersistentFloat32
		Mentira bool
	}

	pCarlos := &pessoa{
		Nome:   NewPersistentString("Carlos"),
		Idade:  NewPersistentUint32(21),
		Altura: NewPersistentFloat32(1.79),
	}

	PersistStruct("pcarlos", pCarlos, storager, func(e error) {
		if e != nil {
			t.Fatal(e)
		}
	})

	for k := range storager.db {
		t.Log(k)
	}

	nCarlos := &pessoa{}

	RestoreStruct("pcarlos", nCarlos, storager, func(e error) {
		if e != nil {
			t.Fatal(e)
		}
	})

	if *pCarlos.Nome != *nCarlos.Nome ||
		*pCarlos.Altura != *nCarlos.Altura ||
		*pCarlos.Idade != *nCarlos.Idade {

		t.Fail()
	}
}
