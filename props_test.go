package riak

import (
	"json"
	"testing"
)

func TestQuorumStrings(t *testing.T){
	out, err := json.Marshal(QUORUM)
	fatalIf(t, err != nil, "Couldn't marshal QuorumValue: %v", err)
	fatalIf(t, string(out) != `"quorum"`, "Got wrong value: '%s'", out)
	out, err = json.Marshal(ALL)
	fatalIf(t, err != nil, "Couldn't marshal QuorumValue: %v", err)
	fatalIf(t, string(out) != `"all"`, "Got wrong value: '%s'", out)
	out, err = json.Marshal(QuorumValue(4))
	fatalIf(t, err != nil, "Couldn't marshal QuorumValue: %v", err)
	fatalIf(t, string(out) != `4`, "Got wrong value: '%s'", out)
}


func TestUnmarshal(t *testing.T){
	props := Properties{}
	err := json.Unmarshal([]byte(`{"name":"test","n_val":3,"allow_mult":false,"last_write_wins":false,"precommit":[],"postcommit":[],"chash_keyfun":{"mod":"riak_core_util","fun":"chash_std_keyfun"},"linkfun":{"mod":"riak_kv_wm_link_walker","fun":"mapreduce_linkfun"},"old_vclock":86400,"young_vclock":20,"big_vclock":50,"small_vclock":10,"r":"quorum","w":"quorum","dw":"quorum","rw":"quorum"}`), &props)
	fatalIf(t, err != nil, "Couldn't marshal QuorumValue: %v", err)
	fatalIf(t, props.R == nil , "R is nil!")
	fatalIf(t, props.DW == nil , "DW is nil!")
	fatalIf(t, props.RW == nil , "RW is nil!")
	fatalIf(t, props.W == nil , "W is nil!")
	fatalIf(t, *props.R != QUORUM, "Got wrong R property: %v", props.R)
	fatalIf(t, *props.DW != QUORUM, "Got wrong DW property: %v", props.DW)
	fatalIf(t, *props.RW != QUORUM, "Got wrong RW property: %v", props.RW)
	fatalIf(t, *props.W != QUORUM, "Got wrong W property: %v", props.W)
	fatalIf(t, props.Name != "test", "Got wrong name property: %v", props.Name)
	fatalIf(t, props.LastWriteWins == nil , "lastwrite wins is nil")
	fatalIf(t, props.AllowMulti == nil , "Allow multi is nil")
	fatalIf(t, *props.AllowMulti, "Got wrong allow-multi property %v", props.AllowMulti)
	fatalIf(t, *props.LastWriteWins, "Got wrong allow-multi property %v", props.LastWriteWins)
}


