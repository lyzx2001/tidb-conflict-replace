package main

import "testing"

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
	mvcc, ok := store.getLatest("r1")
	if !ok {
		t.Fatal()
	}
	if mvcc.isDelete {
		t.Fatal()
	}
}
