#ifdef PROMETHEUS

#ifndef __PROMETHEUS_REPORTER_HH__
#define __PROMETHEUS_REPORTER_HH__

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

using namespace prometheus;

namespace bm {
class PrometheusReporter {
private:
    Exposer* _exposer;
    std::shared_ptr<Registry> _registry;

public:
    // Family
    Family<Counter>& counter_family;
    Family<Gauge>& gauge_family;
    Family<Summary>& summary_family;

    PrometheusReporter(
        std::shared_ptr<Registry> registry, 
        Family<Counter>& counter_family, 
        Family<Gauge>& gauge_family,
        Family<Summary>& summary_family
    );
    ~PrometheusReporter();
};
} // namespace bm

#endif 

#endif