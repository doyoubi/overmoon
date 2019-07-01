package broker

type proxyMeta struct {
	proxyIndex    uint64
	clusterName   string
	nodeAddresses []string
}

type failedProxyMeta struct {
	nodeAddresses []string
}

type failureReportMeta struct {
	reportTime int64
}

type nodeMeta struct {
	nodeAddress  string
	proxyAddress string
	slots        []slotRangeMeta
}

type slotRangeMeta struct {
	start uint64
	end   uint64
	tag   slotRangeTagMeta
}

type migrationTagType string

type slotRangeTagMeta struct {
	tagType migrationTagType
	meta    migrationMeta
}

type migrationMeta struct {
	meta          uint64
	srcProxyIndex uint64
	dstProxyIndex uint64
}
