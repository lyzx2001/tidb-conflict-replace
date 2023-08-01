package main

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func checkConsistKey(t *testing.T, store *KvStore, key string) {
	mvcc, ok := store.getLatest(key)
	if !ok {
		t.Fatal()
	}
	if mvcc.isDelete {
		t.Fatal()
	}
}

func TestCover1(t *testing.T) {
	rows := []string{
		"1,A,a",
		"1,A,b",
	}
	store := initializeKVStore(rows)
	numOfKV, err := checkConsistConflict(store)
	if err == nil {
		t.FailNow()
	}
	require.Equal(t, 0, numOfKV)
	replaceConflict(store)
	numOfKV, err = checkConsistConflict(store)
	if err != nil {
		t.Fatal(err.Error())
	}
	require.Equal(t, 3, numOfKV)
	checkConsistKey(t, store, "r1")
	checkConsistKey(t, store, "i1_A")
	checkConsistKey(t, store, "i2_b")
}

func TestCover2(t *testing.T) {
	rows := []string{
		"1,A,a",
		"1,A,b",
		"1,B,a",
		"2,C,c",
		"2,C,d",
		"2,D,d",
	}
	store := initializeKVStore(rows)
	numOfKV, err := checkConsistConflict(store)
	if err == nil {
		t.FailNow()
	}
	require.Equal(t, 0, numOfKV)
	replaceConflict(store)
	numOfKV, err = checkConsistConflict(store)
	if err != nil {
		t.Fatal(err.Error())
	}
	require.Equal(t, 6, numOfKV)
	checkConsistKey(t, store, "r1")
	checkConsistKey(t, store, "r2")
	checkConsistKey(t, store, "i1_B")
	checkConsistKey(t, store, "i1_D")
	checkConsistKey(t, store, "i2_a")
	checkConsistKey(t, store, "i2_d")
}

func TestCoverWithMock(t *testing.T) {
	rows := []string{
		"1,A,a",
		"1,A,b",
		"1,B,a",
		"1,B,b",
		"2,A,a",
		"2,A,b",
		"2,B,a",
		"2,B,b",
	}
	rowsForMock := [][]string{
		{"1", "A", "a"},
		{"1", "A", "b"},
		{"1", "B", "a"},
		{"1", "B", "b"},
		{"2", "A", "a"},
		{"2", "A", "b"},
		{"2", "B", "a"},
		{"2", "B", "b"},
	}
	store := initializeKVStore(rows)
	numOfKV, err := checkConsistConflict(store)
	if err == nil {
		t.FailNow()
	}
	require.Equal(t, 0, numOfKV)
	replaceConflict(store)
	numOfKV, err = checkConsistConflict(store)
	if err != nil {
		t.Fatal(err.Error())
	}
	require.Equal(t, 3, numOfKV)
	mockNumOfKV := mockInsertReplace(rowsForMock)
	require.Equal(t, 6, mockNumOfKV)
}
