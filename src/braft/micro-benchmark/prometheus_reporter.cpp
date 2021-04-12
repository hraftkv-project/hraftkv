#ifdef PROMETHEUS

#include "prometheus_reporter.h"

using namespace prometheus;

namespace bm {

PrometheusReporter::PrometheusReporter(
    std::shared_ptr<Registry> registry,
    Family<Counter>& counter_family, 
    Family<Gauge>& gauge_family,
    Family<Summary>& summary_family) : 
    _registry(registry),
    counter_family(counter_family),
    gauge_family(gauge_family),
    summary_family(summary_family)
{
    _exposer = new Exposer("0.0.0.0:9000");
    // _registry = std::make_shared<Registry>();

    // ask the exposer to scrape the registry on incoming HTTP requests
    _exposer->RegisterCollectable(_registry);
}

PrometheusReporter::~PrometheusReporter() {
    delete _exposer;
    _registry.reset();
}

} // namespace bm

#endif