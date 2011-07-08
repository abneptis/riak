package riak

import (
	"json"
	"os"
)

type QuorumValue int

const (
	QUORUM	QuorumValue =	-iota
	ALL
)

func (self QuorumValue)MarshalJSON()(out []byte, err os.Error){
	switch self {
		case QUORUM:	out, err = json.Marshal("quorum")
		case ALL:			out, err = json.Marshal("all")
		default:			out, err = json.Marshal(int(self))
	}
	return
}

func (self *QuorumValue)UnmarshalJSON(in []byte)(err os.Error){
	var s string
	err = json.Unmarshal(in, &s)
	if err == nil {
		switch s {
			case "quorum": *self = QUORUM
			case "all": *self = ALL
			default: err = os.NewError("Unexpected string quorum value")
		}
	} else {
		var i int
		err = json.Unmarshal(in, &i)
		if err == nil {
			*self = QuorumValue(i)
		}
	}
	return
}

type Properties struct {
	NVal	int	"n_val"
	AllowMulti	*bool	"allow_mult"
	LastWriteWins	*bool	"last_write_wins"
	R	*QuorumValue	"r"
	W	*QuorumValue	"w"
	DW	*QuorumValue	"dw"
	RW	*QuorumValue	"rw"
	Backend string "backend"
	PreCommit	[]string	"precommit"
	PostCommit	[]string	"postcommit"
	// 'read-only' parameters (retrieved via GetBucketInfo)
	Name string	"name"
	BigVclock	int	"big_vclock"
	SmallVclock int "small_vclock"
	OldVclock	int	"old_vclock"
	YoungVclock int "young_vclock"
}

func DefaultProperties()(Properties){
	return Properties {
		NVal: 3,
		AllowMulti: nil,
		LastWriteWins: nil,
		R: nil,
		W: nil,
		DW: nil,
		RW: nil,
		Backend: "",
	}
}

func (self Properties)MarshalJSON()(out []byte, err os.Error){
	omap := map[string]interface{}{}
	if self.Backend != "" { omap["backend"] = self.Backend }
	if self.R != nil { omap["r"] = self.R }
	if self.W != nil { omap["w"] = self.W }
	if self.DW != nil { omap["dw"] = self.DW }
	if self.RW != nil { omap["rw"] = self.RW }
	if self.PreCommit != nil { omap["precommit"] = self.PreCommit }
	if self.PostCommit != nil { omap["postcommit"] = self.PostCommit }
	if self.AllowMulti != nil { omap["allow_mult"] = *self.AllowMulti } 
	if self.LastWriteWins != nil { omap["last_write_wins"] = *self.LastWriteWins } 
	out, err = json.Marshal(omap)
	return 
}
