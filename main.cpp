#include <fstream>
#include <filesystem>
#include <iostream>
#include <memory>
#include <atomic>
#include <list>

#include <boost/program_options.hpp>

#include "common.h"

using namespace std;

bool WaitForPulsar(const Config& conf) {
    ostringstream topic;
    topic << "persistent:/"
        << '/' << conf.tenant_name
        << '/' << conf.namespace_name
        << '/' << "probe";

    auto topic_str = topic.str();

    pulsar::ClientConfiguration cc;
    cc.setUseTls(false);
    cc.setLogger({});
    pulsar::Client pclient{conf.pulsar_url, cc};

    for(int i = 0; i < 60; ++i) {
        pulsar::Consumer consumer;
        const auto res = pclient.subscribe(topic_str, "me", consumer);
        if (res != pulsar::ResultOk) {
            this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        consumer.close();
        return true;
    }

    return false;
}

int main(int argc, char *argv[])
{
    namespace po = boost::program_options;
    namespace logging = boost::log;

    Config config;
    string log_level;

    po::options_description general("General Options");

    general.add_options()
        ("help,h", "Print help and exit")
        ("version", "Print version and exit")
        ("log-level,l", po::value<string>(&log_level)->default_value("info"),
            "Log-level for the log-file")
        ;

    po::options_description testopts("Test Options");
    testopts.add_options()
        ("pulsar-threads", po::value<size_t>(&config.pulsar_client_threads)->default_value(config.pulsar_client_threads),
            "Number of IO threads used by the Pulsar client.")
        ("asio-threads", po::value<size_t>(&config.asio_threads)->default_value(config.asio_threads),
            "Number of threads used by asio for the producer.")
        ("messages", po::value<size_t>(&config.messages)->default_value(config.messages),
            "Number of messages to send on each topic. 0 for unlimited.")
        ("duration", po::value<size_t>(&config.test_duration)->default_value(config.test_duration),
            "Number of seconds to produce messages before the producers stop. 0 for unlimited.")
        ("message-size", po::value<size_t>(&config.message_size)->default_value(config.message_size),
            "Size of each message.")
        ("messages-per-second", po::value<size_t>(&config.produce_messages_per_second)->default_value(config.produce_messages_per_second),
            "How many messages to produce per second for each topic.")
        ("producer", po::value<bool>(&config.producer)->default_value(config.producer),
            "Produser switch.")
        ("consumers,c", po::value<size_t>(&config.consumers)->default_value(config.consumers),
            "Number of consumers.")
        ("max-consumers-per-client", po::value<size_t>(&config.consumers_per_client)->default_value(config.consumers_per_client),
            "Max number of consumers per Pulsar Client instance.")
        ("producer-batching", po::value<bool>(&config.producer_batching)->default_value(config.producer_batching)->default_value(config.producer_batching),
            "Consumer batching switch.")
        ("stats_interval", po::value<int>(&config.stats_interval)->default_value(config.stats_interval),
            "Interval (in seconds) between stats dumps in the log. 0 to disable.")
        ("report-file", po::value<string>(&config.report_file)->default_value(config.report_file),
            "CSV file to write resultys to.")
        ("storage", po::value<string>(&config.storage)->default_value(config.storage),
            "Storage used (info).")
        ("where", po::value<string>(&config.location)->default_value(config.location),
            "Where the tests were ran (info).")
        ("pulsar-deployment-cpus", po::value<string>(&config.pulsar_deployment_cpus)->default_value(config.pulsar_deployment_cpus),
            "How many CPU's available to Pulsar during the test (info).")
        ("pulsar-deployment-ram", po::value<string>(&config.pulsar_deployment_ram)->default_value(config.pulsar_deployment_ram),
            "How much RAM was available to Pulsar during the test (info).")
        ;

    po::options_description confopts("Config Options");
    confopts.add_options()
        ("pulsar-url", po::value<string>(&config.pulsar_url)->default_value(config.pulsar_url),
            "Pulsar service url.")
        ("cluster-name", po::value<string>(&config.cluster_name)->default_value(config.cluster_name),
            "Cluster name")
        ("tenant-name", po::value<string>(&config.tenant_name)->default_value(config.tenant_name),
            "Tenant name")
        ("namespace", po::value<string>(&config.namespace_name)->default_value(config.namespace_name),
            "Namepsace to use.")
        ("topic-name", po::value<string>(&config.topic_name)->default_value(config.topic_name),
            "Topic-name.")
        ;

    po::options_description cmdline_options;
    cmdline_options.add(general).add(testopts).add(confopts);

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, cmdline_options), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << cmdline_options << endl
            << "Log-levels are:" << endl
            << "   trace debug info " << endl;

        return -2;
    }

    if (vm.count("version")) {
        cout << "pulsartest " << PROJECT_VERSION << endl;
        return -2;
    }


    auto llevel = logging::trivial::info;
    if (log_level == "debug") {
        llevel = logging::trivial::debug;
    } else if (log_level == "trace") {
        llevel = logging::trivial::trace;
    } else if (log_level == "info") {
        ; // Do nothing
    } else {
        std::cerr << "Unknown log-level: " << log_level << endl;
        return  -1;
    }

    logging::core::get()->set_filter
    (
        logging::trivial::severity >= llevel
    );

    if (!config.producer && !config.consumers) {
        BOOST_LOG_TRIVIAL( error) << "Nothing to do. I'm neither a producer nor a cosnumer!";
        return -3;
    }

    try {
        // Wait for pulsar to come up (as needed)
        if (!WaitForPulsar(config)) {
            BOOST_LOG_TRIVIAL ( error ) << "Cannot subscribe to Pulsar.";
            return -1;
        }


        Base::ptr_t consumer;
        Base::ptr_t producer;
        std::list<std::future<void>> futures;
        boost::asio::io_context ctx;
        std::list<std::thread> workers;

        BOOST_LOG_TRIVIAL( info ) << "Getting myself ready...";
        boost::asio::io_service::work asio_work{ctx};

        for(size_t t = 0; t < config.asio_threads; ++t) {
            thread thd([t, &ctx]() {
                BOOST_LOG_TRIVIAL ( debug ) << "Starting asio loop " << t;
                auto name = "asio-"s + to_string(t);
                SetThreadName(name.c_str());
                ctx.run();
                BOOST_LOG_TRIVIAL ( debug ) << "Done with asio loop " << t;
            });

            workers.push_back(move(thd));
        }

        SetThreadName("main");
        Timer main_timer;

        vector<shared_ptr<Base>> clist;
        if (config.consumers) {
            unsigned cid = 1;
            for (size_t num = config.consumers; num;
                 num -= min(num, config.consumers_per_client)) {

                clist.emplace_back(Base::CreateConsumer(ctx, config, cid,
                                                        min(num, config.consumers_per_client)));
                futures.push_back(clist.back()->Run());
                ++cid;
            }
        }

        if (config.producer) {
            producer = Base::CreateProducer(ctx, config);
            futures.push_back(producer->Run());
        }

        // Wait patiently for the tests to complete...
        for(auto& f: futures) {
            f.get();
        }

        ctx.stop();

        for(auto& w : workers) {
            w.join();
        }

        const auto elapsed = main_timer.elapsedSeconds();

        if (!filesystem::exists(config.report_file)) {
            std::ofstream hdr{config.report_file};
            hdr << "mps, messages, streams, pulsar-threads, asio-threads, batching, s-duration, s-ok, s-failed, s-avg, s-total-avg, "
                << "r-duration, r-ok, r-failed, f-failed-ack, r-avg, r-total-avg, app-time, "
                << "storage, where, pulsar-cpus, pulsar-ram"
                << endl;
        }
        {
            std::ofstream csv{config.report_file, ofstream::app};
            csv << fixed
                << config.produce_messages_per_second << ','
                << config.messages << ','
                << config.pulsar_client_threads  << ','
                << config.asio_threads << ','
                << config.producer_batching << ','

                << producer->GetResults().duration_ << ','
                << producer->GetResults().ok_messages << ','
                << producer->GetResults().failed_messages << ','
                << producer->GetResults().avg_per_sec << ','
                << producer->GetResults().aggregated_avg_per_sec << ','

//                << consumer->GetResults().duration_ << ','
//                << consumer->GetResults().ok_messages << ','
//                << consumer->GetResults().failed_messages << ','
//                << consumer->GetResults().failed_ack << ','
//                << consumer->GetResults().avg_per_sec << ','
//                << consumer->GetResults().aggregated_avg_per_sec << ','
//                << elapsed << ','

                << config.storage << ','
                << config.location << ','
                << config.pulsar_deployment_cpus << ','
                << config.pulsar_deployment_ram

                << endl;
        }

        BOOST_LOG_TRIVIAL( info ) << "Done after " << elapsed << " seconds";

    } catch (const std::exception& ex) {
        BOOST_LOG_TRIVIAL( error) << "Caught exacption in main: " << ex.what();
        return -5;
    }

    return 0;
}
