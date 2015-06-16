package memdb

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"testing"
)

type TestObject struct {
	ID    string
	Foo   string
	Bar   int
	Baz   string
	Empty string
}

func testObj() *TestObject {
	obj := &TestObject{
		ID:  "my-cool-obj",
		Foo: "Testing",
		Bar: 42,
		Baz: "yep",
	}
	return obj
}

func TestStringFieldIndex_FromObject(t *testing.T) {
	obj := testObj()
	indexer := StringFieldIndex{"Foo", false}

	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "Testing" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	lower := StringFieldIndex{"Foo", true}
	ok, val, err = lower.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "testing" {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := StringFieldIndex{"NA", true}
	ok, val, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	emptyField := StringFieldIndex{"Empty", true}
	ok, val, err = emptyField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not ok")
	}
}

func TestStringFieldIndex_FromArgs(t *testing.T) {
	indexer := StringFieldIndex{"Foo", false}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	val, err := indexer.FromArgs("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo" {
		t.Fatalf("foo")
	}

	lower := StringFieldIndex{"Foo", true}
	val, err = lower.FromArgs("Foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(val) != "foo" {
		t.Fatalf("foo")
	}
}

func TestUUIDFeldIndex_parseString(t *testing.T) {
	u := &UUIDFieldIndex{}
	_, err := u.parseString("invalid")
	if err == nil {
		t.Fatalf("should error")
	}

	buf, uuid := generateUUID()

	out, err := u.parseString(uuid)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !bytes.Equal(out, buf) {
		t.Fatalf("bad: %#v %#v", out, buf)
	}
}

func TestUUIDFieldIndex_FromObject(t *testing.T) {
	obj := testObj()
	uuidBuf, uuid := generateUUID()
	obj.Foo = uuid
	indexer := &UUIDFieldIndex{"Foo"}

	ok, val, err := indexer.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("bad: %s", val)
	}
	if !ok {
		t.Fatalf("should be ok")
	}

	badField := &UUIDFieldIndex{"NA"}
	ok, val, err = badField.FromObject(obj)
	if err == nil {
		t.Fatalf("should get error")
	}

	emptyField := &UUIDFieldIndex{"Empty"}
	ok, val, err = emptyField.FromObject(obj)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("should not ok")
	}
}

func TestUUIDFieldIndex_FromArgs(t *testing.T) {
	indexer := &UUIDFieldIndex{"Foo"}
	_, err := indexer.FromArgs()
	if err == nil {
		t.Fatalf("should get err")
	}

	_, err = indexer.FromArgs(42)
	if err == nil {
		t.Fatalf("should get err")
	}

	uuidBuf, uuid := generateUUID()

	val, err := indexer.FromArgs(uuid)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("foo")
	}

	val, err = indexer.FromArgs(uuidBuf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !bytes.Equal(uuidBuf, val) {
		t.Fatalf("foo")
	}
}

func generateUUID() ([]byte, string) {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	uuid := fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
	return buf, uuid
}