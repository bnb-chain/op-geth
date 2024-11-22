// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

type accessList struct {
	addresses map[common.Address]int
	slots     []map[common.Hash]struct{}
}

// ContainsAddress returns true if the address is in the access list.
func (al *accessList) ContainsAddress(address common.Address) bool {
	_, ok := al.addresses[address]
	return ok
}

// Contains checks if a slot within an account is present in the access list, returning
// separate flags for the presence of the account and the slot respectively.
func (al *accessList) Contains(address common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	idx, ok := al.addresses[address]
	if !ok {
		// no such address (and hence zero slots)
		return false, false
	}
	if idx == -1 {
		// address yes, but no slots
		return true, false
	}
	_, slotPresent = al.slots[idx][slot]
	return true, slotPresent
}

// newAccessList creates a new accessList.
func newAccessList() *accessList {
	return &accessList{
		addresses: make(map[common.Address]int),
	}
}

// Copy creates an independent copy of an accessList.
func (a *accessList) Copy() *accessList {
	cp := newAccessList()
	for k, v := range a.addresses {
		cp.addresses[k] = v
	}
	cp.slots = make([]map[common.Hash]struct{}, len(a.slots))
	for i, slotMap := range a.slots {
		newSlotmap := make(map[common.Hash]struct{}, len(slotMap))
		for k := range slotMap {
			newSlotmap[k] = struct{}{}
		}
		cp.slots[i] = newSlotmap
	}
	return cp
}

// AddAddress adds an address to the access list, and returns 'true' if the operation
// caused a change (addr was not previously in the list).
func (al *accessList) AddAddress(address common.Address) bool {
	if _, present := al.addresses[address]; present {
		return false
	}
	al.addresses[address] = -1
	return true
}

// AddSlot adds the specified (addr, slot) combo to the access list.
// Return values are:
// - address added
// - slot added
// For any 'true' value returned, a corresponding journal entry must be made.
func (al *accessList) AddSlot(address common.Address, slot common.Hash) (addrChange bool, slotChange bool) {
	idx, addrPresent := al.addresses[address]
	if !addrPresent || idx == -1 {
		// Address not present, or addr present but no slots there
		al.addresses[address] = len(al.slots)
		slotmap := map[common.Hash]struct{}{slot: {}}
		al.slots = append(al.slots, slotmap)
		return !addrPresent, true
	}
	// There is already an (address,slot) mapping
	slotmap := al.slots[idx]
	if _, ok := slotmap[slot]; !ok {
		slotmap[slot] = struct{}{}
		// Journal add slot change
		return false, true
	}
	// No changes required
	return false, false
}

// DeleteSlot removes an (address, slot)-tuple from the access list.
// This operation needs to be performed in the same order as the addition happened.
// This method is meant to be used  by the journal, which maintains ordering of
// operations.
func (al *accessList) DeleteSlot(address common.Address, slot common.Hash) {
	idx, addrOk := al.addresses[address]
	// There are two ways this can fail
	if !addrOk {
		panic("reverting slot change, address not present in list")
	}
	slotmap := al.slots[idx]
	delete(slotmap, slot)
	// If that was the last (first) slot, remove it
	// Since additions and rollbacks are always performed in order,
	// we can delete the item without worrying about screwing up later indices
	if len(slotmap) == 0 {
		al.slots = al.slots[:idx]
		al.addresses[address] = -1
	}
}

// DeleteAddress removes an address from the access list. This operation
// needs to be performed in the same order as the addition happened.
// This method is meant to be used  by the journal, which maintains ordering of
// operations.
func (al *accessList) DeleteAddress(address common.Address) {
	delete(al.addresses, address)
}

// Copy creates an independent copy of an accessList.
func (dest *accessList) Append(src *accessList) *accessList {
	for addr, sIdx := range src.addresses {
		if i, present := dest.addresses[addr]; present {
			// dest already has addr.
			if sIdx >= 0 {
				// has slot in list
				if i == -1 {
					dest.addresses[addr] = len(dest.slots)
					slotmap := src.slots[sIdx]
					dest.slots = append(dest.slots, slotmap)
				} else {
					slotmap := src.slots[sIdx]
					for hash := range slotmap {
						if _, ok := dest.slots[i][hash]; !ok {
							dest.slots[i][hash] = struct{}{}
						}
					}
				}
			}
		} else {
			// dest doesn't have the address
			dest.addresses[addr] = -1
			if sIdx >= 0 {
				dest.addresses[addr] = len(dest.slots)
				slotmap := src.slots[sIdx]
				dest.slots = append(dest.slots, slotmap)
			}
		}
	}
	return dest
}

type parallelAccessList struct {
	addresses *sync.Map
	slots     []*sync.Map
}

func (p *parallelAccessList) ContainsAddress(address common.Address) bool {
	_, ok := p.addresses.Load(address)
	return ok
}

func (p *parallelAccessList) Contains(address common.Address, slot common.Hash) (bool, bool) {
	idx, ok := p.addresses.Load(address)
	if !ok {
		// no such address (and hence zero slots)
		return false, false
	}
	if idx == -1 {
		// address yes, but no slots
		return true, false
	}
	_, slotPresent := p.slots[idx.(int)].Load(slot)
	return true, slotPresent
}

func (p *parallelAccessList) AddAddress(address common.Address) bool {
	if _, present := p.addresses.Load(address); present {
		return false
	}
	p.addresses.Store(address, -1)
	return true
}

func (p *parallelAccessList) AddSlot(address common.Address, slot common.Hash) (addrChange bool, slotChange bool) {
	idx, addrPresent := p.addresses.Load(address)
	if !addrPresent || idx == -1 {
		// Address not present, or addr present but no slots there
		p.addresses.Store(address, len(p.slots))
		slotmap := sync.Map{}
		slotmap.Store(slot, struct{}{})
		p.slots = append(p.slots, &slotmap)
		return !addrPresent, true
	}
	// There is already an (address,slot) mapping
	slotmap := p.slots[idx.(int)]
	if _, ok := slotmap.Load(slot); !ok {
		slotmap.Store(slot, struct{}{})
		// Journal add slot change
		return false, true
	}
	// No changes required
	return false, false
}

func (p *parallelAccessList) Copy() *accessList {
	cp := newAccessList()
	p.addresses.Range(func(key, value interface{}) bool {
		cp.addresses[key.(common.Address)] = value.(int)
		return true
	})
	cp.slots = make([]map[common.Hash]struct{}, len(p.slots))
	for i, slotMap := range p.slots {
		newSlotmap := make(map[common.Hash]struct{})
		slotMap.Range(func(key, value interface{}) bool {
			newSlotmap[key.(common.Hash)] = struct{}{}
			return true
		})
		cp.slots[i] = newSlotmap
	}
	return cp
}

func (p *parallelAccessList) setByAccessList(list *accessList) {
	p.addresses = &sync.Map{}
	for key, value := range list.addresses {
		p.addresses.Store(key, value)
	}
	p.slots = make([]*sync.Map, len(list.slots))
	for idx, slot := range list.slots {
		slotmap := sync.Map{}
		for key := range slot {
			slotmap.Store(key, struct{}{})
		}
		p.slots[idx] = &slotmap
	}
}

// newAccessList creates a new accessList.
func newParallelAccessList() *parallelAccessList {
	return &parallelAccessList{
		addresses: &sync.Map{},
	}
}
