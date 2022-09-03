package cron_worker

import (
	"errors"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io"
	"net/http"
	"os"
	"time"
)

type (
	Config struct {
		File          string
		EndPoint      string
		WarnThreshold time.Duration
	}
	Worker struct {
		config  *Config
		crontab *sqsCron

		cron *cron.Cron
	}
	sqsCronItem struct {
		Name        string `yaml:"name"`
		Url         string `yaml:"url"`
		Schedule    string `yaml:"schedule"`
		cronEntryId cron.EntryID
	}
	sqsCron struct {
		Version int `yaml:"version"`
		Cron    []sqsCronItem
	}
)

func New(c *Config) *Worker {
	if "" == c.File {
		log.Info("No Cron File Supplied")
		return nil
	}

	wkr := Worker{
		config:  c,
		crontab: nil,
	}
	wkr.loadCronTab()

	return &wkr
}

func (w *Worker) Run() {
	if nil == w.cron {
		log.Errorf("Cannot run cron")
		return
	}
	w.cron.Start()
	for _, entry := range w.crontab.Cron {
		log.
			WithField("what", "cron").
			WithField("name", entry.Name).
			WithField("next", w.cron.Entry(entry.cronEntryId).Next).
			Debug("Next Occurrence")
	}

	// TODO: wait on w.config.File changes and reload the crontab
}

func (w *Worker) Stop() {
	w.cron.Stop()
}

// loadCronTab is the parent method that reads, parses and then loads the crontab
func (w *Worker) loadCronTab() {
	contents, err := w.readCronTab(w.config.File)
	if nil != err {
		log.WithError(err).Error("Failed to load crontab")
		return
	}

	if err = w.parseCronTab(contents); nil != err {
		log.WithError(err).Error("Failed to parse crontab")
		return
	}

	// log some info about our crontab
	log.
		WithField("file", w.config.File).
		WithField("version", w.crontab.Version).
		WithField("num-entries", len(w.crontab.Cron)).
		Info("EBS Crontab Info")

	if err = w.loadCronEntries(); nil != err {
		log.WithError(err).Error("Failed to start crontab")
		return
	}
}

func (w *Worker) readCronTab(path string) ([]byte, error) {
	fh, err := os.Open(path)
	if nil != err {
		return nil, err
	}

	return io.ReadAll(fh)
}

func (w *Worker) parseCronTab(contents []byte) error {
	crontab := sqsCron{}

	err := yaml.Unmarshal(contents, &crontab)
	if nil != err {
		return err
	}

	w.crontab = &crontab
	return nil
}

func (w *Worker) loadCronEntries() error {
	if nil == w.crontab {
		return errors.New("please parse a crontab before loading it")
	}

	w.cron = cron.New()

	for idx, entry := range w.crontab.Cron {
		log.WithField("entry", entry.Name).Debug("Adding Cron Entry")
		entryId, err := w.cron.AddFunc(entry.Schedule, w.makeCronRequestFunc(entry))
		if err != nil {
			log.
				WithField("what", "cron").
				WithError(err).
				WithField("entry", entry.Name).
				Error("Failed to load cron entry")
			continue
		}
		w.crontab.Cron[idx].cronEntryId = entryId
	}

	return nil
}

func (w *Worker) makeCronRequestFunc(entry sqsCronItem) func() {
	cronUrl := w.config.EndPoint + entry.Url
	return func() {
		t1 := time.Now()
		rqLog := log.
			WithField("what", "cron").
			WithField("entry", entry.Name).
			WithField("url", cronUrl).
			WithField("start", t1)

		rqLog.Debug("Requesting Cron URL")

		resp, err := http.Post(cronUrl, "application/json", nil)
		if err != nil {
			rqLog.
				WithError(err).
				Error("Failed Requesting Endpoint")
			return
		}
		rqLog.WithField("http-status", resp.StatusCode)

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			rqLog.
				Error("Requesting cron endpoint resulted in non 2XX Status Code")
		}
		t2 := time.Now()

		dur := t2.Sub(t1)

		rqLog.WithField("duration", dur.String())

		if w.config.WarnThreshold > 0 && dur > w.config.WarnThreshold {
			rqLog.Warn("Cron Job Time Taken Exceeded Threshold")
		} else {
			rqLog.Info("Cron Success")
		}
	}
}
