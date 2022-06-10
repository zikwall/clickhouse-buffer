package cx

type imMemoryQueueEngine struct {
	retries chan *RetryPacket
}

func NewImMemoryQueueEngine() Queueable {
	r := &imMemoryQueueEngine{
		retries: make(chan *RetryPacket, defaultRetryChanSize),
	}
	return r
}

func (r *imMemoryQueueEngine) Queue(packet *RetryPacket) {
	r.retries <- packet
}

func (r *imMemoryQueueEngine) Retries() <-chan *RetryPacket {
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
