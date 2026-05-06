package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"
	"log"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	valCur, _, errCur := sck.IKVClerk.Get("currentConfig")
	if errCur != rpc.OK {
		panic("InitController: Get currentConfig error")
	}
	currentConfig := shardcfg.FromString(valCur)
	valNew, _, errNew := sck.IKVClerk.Get("newConfig")
	if errNew == rpc.ErrNoKey {
		return // no new config stored
	}
	newConfig := shardcfg.FromString(valNew)
	if currentConfig.Num == newConfig.Num {
		return // new config already applied
	} else if currentConfig.Num > newConfig.Num {
		panic("InitController: currentConfig.Num > newConfig.Num")
	}

	DPrintf("Controller.InitController: new config detected. Restoring. currentConfig=%+v newConfig=%+v", currentConfig, newConfig)
	sck.ChangeConfigTo(newConfig)

}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	//log.Printf("Conteoller: executing InitConfig with config=%v ...", cfg.String())
	// ErrMayble means ErrVersion received after Put was retried. So either previous attemps succeeded, or another controller called InitConfig() which  is probably not what the tester does.
	// ErrVersion means another controller called InitConfig() which should not be possible.
	result := sck.IKVClerk.Put("currentConfig", cfg.String(), 0) // verison 0 means initial key creation
	if result == rpc.ErrVersion {
		panic("InitConfig received ErrVersion!") // someone already created currentConfig (version > 0)
	}
	//log.Printf("Conteoller: InitConfig executed with config=%v", cfg.String())
}

type ShardMovement struct {
	Shard        shardcfg.Tshid
	CurrentGroup tester.Tgid
	NewGroup     tester.Tgid
}

func (sck *ShardCtrler) storeNewConfig(new *shardcfg.ShardConfig) bool {
	// TODO what if nextConfig exists and is not completed?
	// check value's Num?
	_, version, err := sck.IKVClerk.Get("newConfig")
	if err == rpc.ErrNoKey {
		version = 0
	}
	err = sck.IKVClerk.Put("newConfig", new.String(), version)
	// TODO check err for ErrVersion? and if detected, another controller is present = quit?
	// and if ErrMaybe, call Get to ensure?
	DPrintf("Controller.ChangeConfigTo: error storing new config to kvsrv: %v", err)
	return true
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// TODO 5C
	// While the controller changes the configuration it may be superseded by another
	// controller.
	// ! Probably clerk should be callend with Num determined in shard config param. FreezeShard returns Num in reply.
	// Probably check if it differs from the current. If so, there is a new controller, exit.
	if !sck.storeNewConfig(new) {
		DPrintf("Controller.ChangeConfigTo: error storing new config to kvsrv. Exiting. newConfig=%+v", new)
		return
	}
	DPrintf("Controller.ChangeConfigTo: new config stored in kvsrv. newConfig=%+v", new)
	val, version, err := sck.IKVClerk.Get("currentConfig")
	if err != rpc.OK {
		panic("ChangeConfigTo: Get currentConfig error")
	}
	currentConfig := shardcfg.FromString(val)
	DPrintf("Controller.ChangeConfigTo: working. currentConfig=%+v newConfig=%+v", currentConfig, new)
	shards := make([]*ShardMovement, shardcfg.NShards)
	for i, _ := range shards {
		shards[i] = &ShardMovement{
			Shard:        shardcfg.Tshid(i),
			CurrentGroup: currentConfig.Shards[i],
			NewGroup:     new.Shards[i],
		}
		DPrintf("Controller.ChangeConfigTo: shardMovement=%+v", shards[i])
	}
	// shard movements: we have current and new configs.
	// create a map of partition: partition_id:(current_group, new_group)
	// and if current group != new_group, move partition from current to new group.
	for _, shard := range shards { // move shards
		if shard.CurrentGroup != shard.NewGroup {
			// group Clerks: each group consists of set of servers.
			// And a shard group Clerk must know these servers to be able to find a group leader and rotate leaders.
			// but current and new groups could differ in server sets.
			// probably we need a Clerk per current group, and a Clerk per new group, each itialized with its own set of servers.

			// TODO cache clerks
			currentClerk := shardgrp.MakeClerk(sck.clnt, currentConfig.Groups[shard.CurrentGroup])
			state, err := currentClerk.FreezeShard(shard.Shard, new.Num)
			if err == rpc.ErrWrongGroup { // ErrWrongGroup is returned on Num mismatch. Try another error type?
				DPrintf("Controller.ChangeConfigTo: Shard[%d] FreezeShard err=ErrWrongGroup. Exiting", shard.Shard)
				return // another controller detected: new.Num was smaller than Num that the shard group has seen
			}
			DPrintf("Controller.ChangeConfigTo: Shard[%d] frozen in group [%d]", shard.Shard, shard.CurrentGroup)
			if err != rpc.OK {
				panic(fmt.Sprintf("ChangeConfigTo: freeze error: %v", err))
			}

			if state == nil { // nil state is returned when shard is already deleted, which could happen if the previous controller tried to apply a new config and crashed or partitioned out
				continue
			}
			// if state == nil { // shard doesn't exist on server or Num mismatch
			// 	DPrintf("Controller.ChangeConfigTo: nil state received for Shard[%d]. Exiting", shard.Shard)
			// 	return
			// }

			newClerk := shardgrp.MakeClerk(sck.clnt, new.Groups[shard.NewGroup])
			err = newClerk.InstallShard(shard.Shard, state, new.Num)
			if err != rpc.OK {
				panic(fmt.Sprintf("ChangeConfigTo: install error: %v", err))
			}
			DPrintf("Controller.ChangeConfigTo: Shard[%d] installed on group [%d]", shard.Shard, shard.NewGroup)

			err = currentClerk.DeleteShard(shard.Shard, new.Num)
			if err != rpc.OK {
				panic(fmt.Sprintf("ChangeConfigTo: delete error: %v", err))
			}
			DPrintf("Controller.ChangeConfigTo: Shard[%d] deleted from group [%d]", shard.Shard, shard.CurrentGroup)
		}
	}
	err = sck.IKVClerk.Put("currentConfig", new.String(), version)
	if err == rpc.ErrVersion {
		DPrintf("Controller.ChangeConfigTo: currentConfig NOT updated due to version conflict")
		return // another controller detected
	}
	DPrintf("Controller.ChangeConfigTo: currentConfig updated to new config")
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	value, _, error := sck.IKVClerk.Get("currentConfig")
	if error != rpc.OK {
		panic(fmt.Sprintf("Controller: Get received error: %v", error))
	}
	return shardcfg.FromString(value)
}
