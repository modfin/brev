package metrics

import (
	"context"
	"crypto/subtle"
	"github.com/modfin/brev/tools"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	ServiceName  string        `cli:"default-host"`
	Push         string        `cli:"metrics-push-url"`
	PushInterval time.Duration `cli:"metrics-push-interval"`
	Poll         bool          `cli:"metrics-poll"`
	PollUser     string        `cli:"metrics-poll-basic-auth-user"`
	PollPassword string        `cli:"metrics-poll-basic-auth-pass"`
}

func New(c Config, lc *tools.Logger) *Metrics {
	p := &Metrics{
		config: c,
		logger: lc.New("prometheus"),
	}
	if c.Push != "" {
		p.pusher = push.New(c.Push, c.ServiceName).Gatherer(prometheus.DefaultGatherer)
	}

	return p
}

type Metrics struct {
	done    chan struct{}
	stopped chan struct{}

	config Config
	pusher *push.Pusher
	logger *logrus.Logger

	once sync.Once
}

func (p *Metrics) Start() {
	p.once.Do(func() {
		if p.config.PushInterval.Seconds() < 10 {
			p.config.PushInterval = 1 * time.Minute
		}
		if p.pusher != nil {
			return
		}
		go func() {
			defer close(p.stopped)

			ticker := time.NewTicker(p.config.PushInterval)
			defer ticker.Stop()
			for {
				select {
				case <-p.done:
					return
				case <-ticker.C:
					p.push()
				}
			}
		}()
	})
}
func (p *Metrics) Stop(ctx context.Context) error {
	close(p.done)
	select {
	case <-p.stopped:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (p *Metrics) Register() promauto.Factory {
	return promauto.With(prometheus.DefaultRegisterer)
}

func (p *Metrics) HttpMetrics() http.HandlerFunc {

	if !p.config.Poll {
		p.logger.Infof("metrics polling is disabled")
		return func(writer http.ResponseWriter, request *http.Request) {
			http.Error(writer, "Not Found", http.StatusNotFound)
		}
	}
	p.logger.Infof("metrics polling is enabled")

	if p.config.PollUser != "" || p.config.PollPassword != "" {
		p.logger.WithField("user", p.config.PollUser).Infof("basic auth enabled for metrics polling endpoint")
	}

	return func(writer http.ResponseWriter, request *http.Request) {
		if p.config.PollUser != "" || p.config.PollPassword != "" {
			user, pass, ok := request.BasicAuth()
			if !ok || user != p.config.PollUser || subtle.ConstantTimeCompare([]byte(pass), []byte(p.config.PollPassword)) != 1 {
				http.Error(writer, "Unauthorized.", http.StatusUnauthorized)
				return
			}
		}
		promhttp.Handler().ServeHTTP(writer, request)
	}
}

func (p *Metrics) push() {
	if p.pusher == nil {
		return
	}
	p.logger.Infof("pushing metrics to %s", p.config.Push)
	err := p.pusher.Push()
	if err != nil {
		p.logger.Errorf("failed to push metrics: %v", err)
	}
}

func (p *Metrics) Middleware() func(http.Handler) http.Handler {
	// Create Prometheus metrics

	requests := p.Register().NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests",
		Help: "Number of HTTP requests.",
	}, []string{"method", "path", "status_code"})

	requestsTotal := p.Register().NewCounter(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests.",
		//LabelNames: []string{"method", "path", "status_code"},
	})
	requestDuration := p.Register().NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_ms",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // Adjust buckets as needed
	}, []string{"method", "path", "status_code"})

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Record start time
			startTime := time.Now()

			// Wrap the response writer to capture the status code
			wrappedResponseWriter := &responseWriterWrapper{ResponseWriter: w}

			// Call the next handler
			next.ServeHTTP(wrappedResponseWriter, r)

			// Record metrics
			statusCode := strconv.Itoa(wrappedResponseWriter.statusCode)
			duration := time.Since(startTime).Seconds()
			requestsTotal.Inc()
			requests.WithLabelValues(r.Method, r.URL.Path, statusCode).Inc()

			if statusCode != "404" {
				requestDuration.WithLabelValues(r.Method, r.URL.Path, statusCode).Observe(duration)
			}
		})
	}
}

// responseWriterWrapper wraps the http.ResponseWriter to capture the status code.
type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code.
func (w *responseWriterWrapper) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}
