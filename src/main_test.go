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
	mvcc, ok := store.getLatest("r1")
	if !ok {
		t.Fatal()
	}
	if mvcc.isDelete {
		t.Fatal()
	}
	mvcc, ok = store.getLatest("r2")
	if !ok {
		t.Fatal()
	}
	if mvcc.isDelete {
		t.Fatal()
	}
	mvcc, ok = store.getLatest("i1_B")
	if !ok {
		t.Fatal()
	}
	if mvcc.isDelete {
		t.Fatal()
	}
	mvcc, ok = store.getLatest("i1_D")
	if !ok {
		t.Fatal()
	}
	if mvcc.isDelete {
		t.Fatal()
	}
	mvcc, ok = store.getLatest("i2_a")
	if !ok {
		t.Fatal()
	}
	if mvcc.isDelete {
		t.Fatal()
	}
	mvcc, ok = store.getLatest("i2_d")
	if !ok {
		t.Fatal()
	}
	if mvcc.isDelete {
		t.Fatal()
	}
}
