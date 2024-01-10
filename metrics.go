package main

import (
	"strings"
	"sync"
	"time"

	"github.com/ccremer/fronius-exporter/pkg/fronius"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

const (
	phase1 = "1"
	phase2 = "2"
	phase3 = "3"
)

var (
	namespace           = "fronius"
	namespaceMeter      = namespace + "_meter"
	scrapeDurationGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "scrape_duration_seconds",
		Help:      "Time it took to scrape the device in seconds",
	})
	scrapeErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "scrape_error_count",
		Help:      "Number of scrape errors",
	})

	inverterPowerGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "inverter_power",
		Help:      "Power flow of the inverter in Watt",
	}, []string{"inverter"})
	inverterBatteryChargeGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "inverter_soc",
		Help:      "State of charge of the battery attached to the inverter in percent",
	}, []string{"inverter"})

	sitePowerLoadGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "site_power_load",
		Help:      "Site power load in Watt",
	})
	sitePowerGridGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "site_power_grid",
		Help:      "Site power supplied to or provided from the grid in Watt",
	})
	sitePowerAccuGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "site_power_accu",
		Help:      "Site power supplied to or provided from the accumulator(s) in Watt",
	})
	sitePowerPhotovoltaicsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "site_power_photovoltaic",
		Help:      "Site power from photovoltaic in Watt",
	})

	siteAutonomyRatioGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "site_autonomy_ratio",
		Help:      "Relative autonomy ratio of the site",
	})
	siteSelfConsumptionRatioGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "site_selfconsumption_ratio",
		Help:      "Relative self consumption ratio of the site",
	})

	siteEnergyGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "site_energy_consumption",
		Help:      "Energy consumption in kWh",
	}, []string{"time_frame"})

	siteMPPTVoltageGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "site_mppt_voltage",
		Help:      "Site mppt voltage in V",
	}, []string{"inverter", "mppt"})

	siteMPPTCurrentDCGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "site_mppt_current_dc",
		Help:      "Site mppt current DC in A",
	}, []string{"inverter", "mppt"})

	// smart meter metrics

	smartMeterCurrentAcPower = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceMeter,
		Name:      "current_ac",
		Help:      "Current AC Power Profile in W",
	}, []string{"device", "phase"})

	smartMeterReactiveVarAC = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceMeter,
		Name:      "reactive_power",
		Help:      "Reactive AC Power in Var",
	}, []string{"device", "direction"})

	smartMeterPhaseFrequencyAvg = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceMeter,
		Name:      "phase_avg_frequency",
		Help:      "Average Phase Frequency in Hz",
	}, []string{"device"})

	smartMeterApparentPowerPhases = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceMeter,
		Name:      "phase_apperent_power",
		Help:      "Apparent power per phase in W",
	}, []string{"device", "phase"})

	smartMeterPowerFactorPhase = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceMeter,
		Name:      "phase_power_factor",
		Help:      "Cos Phi power factor per phase",
	}, []string{"device", "phase"})

	smartMeterReactivePowerPhase = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceMeter,
		Name:      "phase_reactive_power",
		Help:      "Reactive power per phase in W",
	}, []string{"device", "phase"})

	smartMeterRealPowerPhase = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceMeter,
		Name:      "phase_real_power",
		Help:      "Real power per phase in W",
	}, []string{"device", "phase"})

	smartMeterPhaseToPhaseVoltage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceMeter,
		Name:      "phase_to_phase_voltage",
		Help:      "Voltage between two phases in V",
	}, []string{"device", "phase1", "phase2"})

	smartMeterVoltage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespaceMeter,
		Name:      "phase_voltage",
		Help:      "Voltage between phase and neutral in V",
	}, []string{"device", "phase"})
)

func collectMetricsFromTarget(client *fronius.SymoClient) {
	start := time.Now()
	log.WithFields(log.Fields{
		"url":              client.Options.URL,
		"timeout":          client.Options.Timeout,
		"powerFlowEnabled": client.Options.PowerFlowEnabled,
		"archiveEnabled":   client.Options.ArchiveEnabled,
		"meterEnabled":     client.Options.SmartMeterEnabled,
	}).Debug("Requesting data.")

	wg := sync.WaitGroup{}
	wg.Add(3)

	collectPowerFlowData(client, &wg)
	collectArchiveData(client, &wg)
	collectSmartMeterData(client, &wg)

	wg.Wait()
	elapsed := time.Since(start)
	scrapeDurationGauge.Set(elapsed.Seconds())
}

func collectPowerFlowData(client *fronius.SymoClient, w *sync.WaitGroup) {
	defer w.Done()
	if client.Options.PowerFlowEnabled {
		powerFlowData, err := client.GetPowerFlowData()
		if err != nil {
			log.WithError(err).Warn("Could not collect Symo power metrics.")
			scrapeErrorCount.Add(1)
			return
		}
		parsePowerFlowMetrics(powerFlowData)
	}
}

func collectArchiveData(client *fronius.SymoClient, w *sync.WaitGroup) {
	defer w.Done()
	if client.Options.ArchiveEnabled {
		archiveData, err := client.GetArchiveData()
		if err != nil {
			log.WithError(err).Warn("Could not collect Symo archive metrics.")
			scrapeErrorCount.Add(1)
			return
		}
		parseArchiveMetrics(archiveData)
	}
}

func collectSmartMeterData(client *fronius.SymoClient, w *sync.WaitGroup) {
	defer w.Done()
	if client.Options.SmartMeterEnabled {
		meterData, err := client.GetMeterData()
		if err != nil {
			log.WithError(err).Warn("Could not collect Symo SmartMeter metrics.")
			scrapeErrorCount.Add(1)
			return
		}
		parseSmartMeterMetrics(meterData)
	}
}

func parsePowerFlowMetrics(data *fronius.SymoData) {
	log.WithField("powerFlowData", *data).Debug("Parsing data.")
	for key, inverter := range data.Inverters {
		inverterPowerGaugeVec.WithLabelValues(key).Set(inverter.Power)
		inverterBatteryChargeGaugeVec.WithLabelValues(key).Set(inverter.BatterySoC / 100)
	}
	sitePowerAccuGauge.Set(data.Site.PowerAccu)
	sitePowerGridGauge.Set(data.Site.PowerGrid)
	sitePowerLoadGauge.Set(data.Site.PowerLoad)
	sitePowerPhotovoltaicsGauge.Set(data.Site.PowerPhotovoltaic)

	siteEnergyGaugeVec.WithLabelValues("day").Set(data.Site.EnergyDay)
	siteEnergyGaugeVec.WithLabelValues("year").Set(data.Site.EnergyYear)
	siteEnergyGaugeVec.WithLabelValues("total").Set(data.Site.EnergyTotal)

	siteAutonomyRatioGauge.Set(data.Site.RelativeAutonomy / 100)
	if data.Site.PowerPhotovoltaic == 0 {
		siteSelfConsumptionRatioGauge.Set(1)
	} else {
		siteSelfConsumptionRatioGauge.Set(data.Site.RelativeSelfConsumption / 100)
	}
}

func parseArchiveMetrics(data map[string]fronius.InverterArchive) {
	log.WithField("archiveData", data).Debug("Parsing data.")
	for key, inverter := range data {
		key = strings.TrimPrefix(key, "inverter/")
		siteMPPTCurrentDCGaugeVec.WithLabelValues(key, "1").Set(inverter.Data.CurrentDCString1.Values["0"])
		siteMPPTCurrentDCGaugeVec.WithLabelValues(key, "2").Set(inverter.Data.CurrentDCString2.Values["0"])
		siteMPPTVoltageGaugeVec.WithLabelValues(key, "1").Set(inverter.Data.VoltageDCString1.Values["0"])
		siteMPPTVoltageGaugeVec.WithLabelValues(key, "2").Set(inverter.Data.VoltageDCString2.Values["0"])
	}
}

func parseSmartMeterMetrics(data map[string]*fronius.SmartMeterData) {
	log.WithField("meterData", data).Debug("Parsing data.")
	for deviceId, meter := range data {
		smartMeterCurrentAcPower.WithLabelValues(deviceId, phase1).Set(meter.CurrentACPhase1)
		smartMeterCurrentAcPower.WithLabelValues(deviceId, phase2).Set(meter.CurrentACPhase2)
		smartMeterCurrentAcPower.WithLabelValues(deviceId, phase3).Set(meter.CurrentACPhase3)
		smartMeterReactiveVarAC.WithLabelValues(deviceId, "consumed").Set(meter.EnergyReactiveVArACSumConsumed)
		smartMeterReactiveVarAC.WithLabelValues(deviceId, "produced").Set(meter.EnergyReactiveVArACSumProduced)
		smartMeterPhaseFrequencyAvg.WithLabelValues(deviceId).Set(meter.FrequencyPhaseAverage)
		smartMeterApparentPowerPhases.WithLabelValues(deviceId, phase1).Set(meter.PowerApparentSPhase1)
		smartMeterApparentPowerPhases.WithLabelValues(deviceId, phase2).Set(meter.PowerApparentSPhase2)
		smartMeterApparentPowerPhases.WithLabelValues(deviceId, phase3).Set(meter.PowerApparentSPhase3)
		smartMeterPowerFactorPhase.WithLabelValues(deviceId, phase1).Set(meter.PowerFactorPhase1)
		smartMeterPowerFactorPhase.WithLabelValues(deviceId, phase2).Set(meter.PowerFactorPhase2)
		smartMeterPowerFactorPhase.WithLabelValues(deviceId, phase3).Set(meter.PowerFactorPhase3)
		smartMeterReactivePowerPhase.WithLabelValues(deviceId, phase1).Set(meter.PowerReactiveQPhase1)
		smartMeterReactivePowerPhase.WithLabelValues(deviceId, phase2).Set(meter.PowerReactiveQPhase2)
		smartMeterReactivePowerPhase.WithLabelValues(deviceId, phase3).Set(meter.PowerReactiveQPhase3)
		smartMeterRealPowerPhase.WithLabelValues(deviceId, phase1).Set(meter.PowerRealPPhase1)
		smartMeterRealPowerPhase.WithLabelValues(deviceId, phase2).Set(meter.PowerRealPPhase2)
		smartMeterRealPowerPhase.WithLabelValues(deviceId, phase3).Set(meter.PowerRealPPhase3)
		smartMeterPhaseToPhaseVoltage.WithLabelValues(deviceId, phase1, phase2).Set(meter.VoltageACPhaseToPhase12)
		smartMeterPhaseToPhaseVoltage.WithLabelValues(deviceId, phase2, phase3).Set(meter.VoltageACPhaseToPhase23)
		smartMeterPhaseToPhaseVoltage.WithLabelValues(deviceId, phase3, phase1).Set(meter.VoltageACPhaseToPhase31)
		smartMeterVoltage.WithLabelValues(deviceId, phase1).Set(meter.VoltageACPhase1)
		smartMeterVoltage.WithLabelValues(deviceId, phase2).Set(meter.VoltageACPhase2)
		smartMeterVoltage.WithLabelValues(deviceId, phase3).Set(meter.VoltageACPhase3)
	}
}
