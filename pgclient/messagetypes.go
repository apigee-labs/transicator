package pgclient

/*
PgOutputType represents the type of an output message to the server.
*/
type PgOutputType int

//go:generate stringer -type PgOutputType .

// Constants representing output message types.
const (
	Bind            PgOutputType = 'B'
	Close           PgOutputType = 'C'
	DescribeMsg     PgOutputType = 'D'
	Execute         PgOutputType = 'E'
	Flush           PgOutputType = 'H'
	Parse           PgOutputType = 'P'
	Query           PgOutputType = 'Q'
	Sync            PgOutputType = 'S'
	Terminate       PgOutputType = 'X'
	CopyDoneOut     PgOutputType = 'c'
	CopyDataOut     PgOutputType = 'd'
	PasswordMessage PgOutputType = 'p'
)

/*
PgInputType is the one-byte type of a postgres response from the server.
*/
type PgInputType int

//go:generate stringer -type PgInputType .

// Various types of messages that represent one-byte message types.
const (
	ErrorResponse          PgInputType = 'E'
	CommandComplete        PgInputType = 'C'
	DataRow                PgInputType = 'D'
	CopyInResponse         PgInputType = 'G'
	CopyOutResponse        PgInputType = 'H'
	EmptyQueryResponse     PgInputType = 'I'
	BackEndKeyData         PgInputType = 'K'
	NoticeResponse         PgInputType = 'N'
	AuthenticationResponse PgInputType = 'R'
	ParameterStatus        PgInputType = 'S'
	RowDescription         PgInputType = 'T'
	CopyBothResponse       PgInputType = 'W'
	ReadyForQuery          PgInputType = 'Z'
	CopyDone               PgInputType = 'c'
	CopyData               PgInputType = 'd'
	HotStandbyFeedback     PgInputType = 'h'
	SenderKeepalive        PgInputType = 'k'
	NoData                 PgInputType = 'n'
	StandbyStatusUpdate    PgInputType = 'r'
	ParameterDescription   PgInputType = 't'
	WALData                PgInputType = 'w'
	ParseComplete          PgInputType = '1'
	BindComplete           PgInputType = '2'
	CloseComplete          PgInputType = '3'
)
