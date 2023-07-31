package main

import "testing"

func checkConsistKey(t *testing.T, store *KvStore, key string) {
	mvcc, ok := store.getLatest(key)
	if !ok {
		t.Fatal()
	}
	if mvcc.isDelete {
		t.Fatal()
	}
}

func TestCover(t *testing.T) {
	rows := []string{
		"1,A,a",
		"1,A,b",
	}
	store := initializeKVStore(rows)
	if checkConsistConflict(store) == nil {
		t.FailNow()
	}
	replaceConflict(store)
	err := checkConsistConflict(store)
	if err != nil {
		t.Fatal(err.Error())
	}
	checkConsistKey(t, store, "r1")
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
	if checkConsistConflict(store) == nil {
		t.FailNow()
	}
	replaceConflict(store)
	err := checkConsistConflict(store)
	if err != nil {
		t.Fatal(err.Error())
	}
	checkConsistKey(t, store, "r1")
	checkConsistKey(t, store, "r2")
	checkConsistKey(t, store, "i1_B")
	checkConsistKey(t, store, "i1_D")
	checkConsistKey(t, store, "i2_a")
	checkConsistKey(t, store, "i2_d")
}
