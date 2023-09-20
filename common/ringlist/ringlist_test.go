package ringlist

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestRingList(t *testing.T) {
	var addrEmpty = common.Address{}
	addr0 := common.HexToAddress("0x0000000000000000000000000000000000000000")
	addr1 := common.HexToAddress("0x0000000000000000000000000000000000000001")
	addr2 := common.HexToAddress("0x0000000000000000000000000000000000000002")
	addr3 := common.HexToAddress("0x0000000000000000000000000000000000000003")
	// case of: queue[0]
	rq := NewRingList[common.Address]()
	rq.Purge()
	if addr := rq.Next(addrEmpty); addr != addrEmpty {
		t.Fatalf("wrong addr returned")
	}

	// case of: queue[1]
	rq = NewRingList[common.Address]()
	rq.Add(addr0)
	rq.Add(common.HexToAddress("0x0000000000000000000000000000000000000000"))
	if addr := rq.Next(addrEmpty); addr != addr0 {
		t.Fatalf("wrong addr returned")
	}
	if addr := rq.Next(addrEmpty); addr != addr0 {
		t.Fatalf("wrong addr returned")
	}
	curr := rq.entry.val
	if rq.entry != rq.preEntry || curr != addr0 || rq.Len() != 1 {
		t.Fatalf("queue should be only one waiting addr: addr1")
	}
	rq.MarkRemoved(addr0)
	rq.Purge()
	if rq.entry != nil || rq.preEntry != nil || len(rq.toRemove) != 0 || rq.Len() != 0 {
		t.Fatalf("should be empty")
	}
	if rq.Next(addrEmpty) != addrEmpty {
		t.Fatalf("wrong addr returned")
	}

	// case of: queue[2]
	rq = NewRingList[common.Address]()
	rq.Add(addr0)
	rq.Add(addr1)
	// case of duplicated address
	rq.Add(common.HexToAddress("0x0000000000000000000000000000000000000000"))
	rq.Add(common.HexToAddress("0x0000000000000000000000000000000000000001"))
	if addr := rq.Next(addrEmpty); addr != addr1 {
		t.Fatalf("wrong addr returned")
	}
	if addr := rq.Next(addrEmpty); addr != addr0 {
		t.Fatalf("wrong addr returned")
	}
	// try another round
	if addr := rq.Next(addrEmpty); addr != addr1 {
		t.Fatalf("wrong addr returned")
	}
	if addr := rq.Next(addrEmpty); addr != addr0 {
		t.Fatalf("wrong addr returned")
	}
	rq.MarkRemoved(addr1)
	rq.Purge()
	curr = rq.entry.val
	if rq.entry != rq.preEntry || curr != addr0 || rq.Len() != 1 {
		t.Fatalf("queue should be only one waiting addr: addr1")
	}
	rq.MarkRemoved(addr0)
	rq.Purge()
	if rq.entry != nil || rq.preEntry != nil || len(rq.toRemove) != 0 || rq.Len() != 0 {
		t.Fatalf("should be empty")
	}

	// case of: queue [3]
	rq = NewRingList[common.Address]()
	rq.Add(addr0)
	rq.Add(addr1)
	rq.Add(addr2)
	if addr := rq.Next(addrEmpty); addr != addr2 {
		t.Fatalf("wrong addr returned")
	}
	if addr := rq.Next(addrEmpty); addr != addr0 {
		t.Fatalf("wrong addr returned")
	}
	if addr := rq.Next(addrEmpty); addr != addr1 {
		t.Fatalf("wrong addr returned")
	}
	if addr := rq.Next(addrEmpty); addr != addr2 {
		t.Fatalf("wrong addr returned")
	}
	if rq.Len() != 3 {
		t.Fatalf("length of waiting queue should be 3")
	}
	rq.MarkRemoved(addr0, addr1, addr2)
	rq.Purge()
	if rq.entry != nil || rq.preEntry != nil || len(rq.toRemove) != 0 || rq.Len() != 0 {
		t.Fatalf("should be empty")
	}

	// Sync(0,1,2) with (1,2,3)
	rq = NewRingList[common.Address]()
	rq.Add(addr0)
	rq.Add(addr1)
	rq.Add(addr2)
	rq.Add(addr3)
	rq.MarkRemoved(common.HexToAddress("0x0000000000000000000000000000000000000000"))
	rq.Purge()
	if addr := rq.Next(addrEmpty); addr != addr3 {
		t.Fatalf("wrong addr returned")
	}
	if addr := rq.Next(addrEmpty); addr != addr1 {
		t.Fatalf("wrong addr returned")
	}
	if addr := rq.Next(addrEmpty); addr != addr2 {
		t.Fatalf("wrong addr returned")
	}
	if rq.Len() != 3 {
		t.Fatalf("length of waiting queue should be 3")
	}
	rq.MarkRemoved(common.HexToAddress("0x0000000000000000000000000000000000000003"))
	rq.Purge()
	if addr := rq.Next(addrEmpty); addr != addr1 {
		t.Fatalf("wrong addr returned")
	}
	if addr := rq.Next(addrEmpty); addr != addr2 {
		t.Fatalf("wrong addr returned")
	}
	if rq.Len() != 2 {
		t.Fatalf("length of waiting queue should be 2")
	}
	rq.MarkRemoved(addr2)
	rq.Purge()
	if addr := rq.Next(addrEmpty); addr != addr1 {
		t.Fatalf("should be addr1")
	}
	if addr := rq.Next(addrEmpty); addr != addr1 {
		t.Fatalf("should be addr1")
	}
	if rq.Len() != 1 {
		t.Fatalf("length of waiting queue should be 1")
	}
	rq.MarkRemoved(addr2, addr1)
	rq.Purge()
	if rq.entry != nil || rq.preEntry != nil || len(rq.toRemove) != 0 || rq.Len() != 0 {
		t.Fatalf("should be empty")
	}

}
