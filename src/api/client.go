package api

type Client interface {
	HandleStream(*Batch) error
	// Writer returns the asynchronous, non-blocking, Write client.
	// Ensures using a single Writer instance for each table pair.
	Writer(view View) Writer
	// Close ensures all ongoing asynchronous write clients finish.
	Close()
}
