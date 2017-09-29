package gorocksdb

import (
	"testing"

	"github.com/facebookgo/ensure"
)

func TestWriteBatch(t *testing.T) {
	db := newTestDB(t, "TestWriteBatch", nil)
	defer db.Close()

	var (
		givenKey1 = []byte("key1")
		givenVal1 = []byte("val1")
		givenKey2 = []byte("key2")
	)
	wo := NewDefaultWriteOptions()
	ensure.Nil(t, db.Put(wo, givenKey2, []byte("foo")))

	// create and fill the write batch
	wb := NewWriteBatch()
	defer wb.Destroy()
	wb.Put(givenKey1, givenVal1)
	wb.Delete(givenKey2)
	ensure.DeepEqual(t, wb.Count(), 2)

	// perform the batch
	ensure.Nil(t, db.Write(wo, wb))

	// check changes
	ro := NewDefaultReadOptions()
	v1, err := db.Get(ro, givenKey1)
	defer v1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v1.Data(), givenVal1)

	v2, err := db.Get(ro, givenKey2)
	defer v2.Free()
	ensure.Nil(t, err)
	ensure.True(t, v2.Data() == nil)
}

func TestWriteBatchPutMany(t *testing.T) {
	db := newTestDB(t, "TestWriteBatchPutMany", nil)
	defer db.Close()

	var (
		key1 = []byte("key1")
		val1 = []byte("val1")
		key2 = []byte("key22")
		val2 = []byte("val22")
	)
	wo := NewDefaultWriteOptions()
	defer wo.Destroy()

	// create and fill the write batch
	keys := [][]byte{key1, key2}
	values := [][]byte{val1, val2}
	wb := NewWriteBatch()
	defer wb.Destroy()
	wb.PutMany(keys, values)
	// ensure.DeepEqual(t, wb.Count(), 2)

	// perform the batch
	ensure.Nil(t, db.Write(wo, wb))

	// check changes
	ro := NewDefaultReadOptions()
	v1, err := db.Get(ro, key1)
	defer v1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v1.Data(), val1)

	v2, err := db.Get(ro, key2)
	defer v2.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v2.Data(), val2)
}

func TestWriteBatchIterator(t *testing.T) {
	db := newTestDB(t, "TestWriteBatchIterator", nil)
	defer db.Close()

	var (
		givenKey1 = []byte("key1")
		givenVal1 = []byte("val1")
		givenKey2 = []byte("key2")
	)
	// create and fill the write batch
	wb := NewWriteBatch()
	defer wb.Destroy()
	wb.Put(givenKey1, givenVal1)
	wb.Delete(givenKey2)
	ensure.DeepEqual(t, wb.Count(), 2)

	// iterate over the batch
	iter := wb.NewIterator()
	ensure.True(t, iter.Next())
	record := iter.Record()
	ensure.DeepEqual(t, record.Type, WriteBatchValueRecord)
	ensure.DeepEqual(t, record.Key, givenKey1)
	ensure.DeepEqual(t, record.Value, givenVal1)

	ensure.True(t, iter.Next())
	record = iter.Record()
	ensure.DeepEqual(t, record.Type, WriteBatchDeletionRecord)
	ensure.DeepEqual(t, record.Key, givenKey2)

	// there shouldn't be any left
	ensure.False(t, iter.Next())
}
