package retry

type imMemoryQueueEngine struct {
	retries chan *Packet
}

func NewImMemoryQueueEngine() Queueable {
	r := &imMemoryQueueEngine{
		retries: make(chan *Packet, defaultRetryChanSize),
	}
	return r
}

func (r *imMemoryQueueEngine) Queue(packet *Packet) {
	r.retries <- packet
}

func (r *imMemoryQueueEngine) Retries() <-chan *Packet {
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
