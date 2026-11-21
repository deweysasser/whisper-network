package store

// FetchSeq is the task sequence number of a fetch task (the download that
// precedes prepare). It is negative so that seq 0 stays the prepare task
// everywhere else, and so that a fetch task sorts before it.
const FetchSeq = -1
