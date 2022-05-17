package clickhousebuffer

type imMemoryQueueEngine struct {
	retries chan *retryPacket
}

func newImMemoryQueueEngine() Queueable {
	r := &imMemoryQueueEngine{
		retries: make(chan *retryPacket, defaultRetryChanSize),
	}
	return r
}

func (r *imMemoryQueueEngine) Queue(packet *retryPacket) {
	r.retries <- packet
}

func (r *imMemoryQueueEngine) Retries() <-chan *retryPacket {
	return r.retries
}

func (r *imMemoryQueueEngine) Close() error {
	if r.retries != nil {
		close(r.retries)
		r.retries = nil
	}
	return nil
}

func (r *imMemoryQueueEngine) CloseMessage() string {
	return "close in-memory queue engine"
}
