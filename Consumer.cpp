
#include "common.h"

using namespace std::string_literals;
using namespace std;

class Receiver : public Base
{
    struct Channel {
        Channel(unsigned cid, size_t id, boost::asio::io_context& ctx)
            : strand_(ctx), id_{id}, cid_{cid}
        {}

        pulsar::Consumer consumer_;
        boost::asio::io_context::strand strand_;

        std::string Name() const {
            return "subscriber-"s + to_string(cid_) + "-"s + to_string(id_);
        }

        auto Id() const noexcept { return id_; }

        size_t GetNumMessagesReceived() const { return messages_received_; }
        size_t GetNumMessagesFailed() const { return messages_failed_; }
        double GetAvgMessagePerSecond() const {
            return messages_received_ / timer_->elapsedSeconds();
        }

        void IncOkMessages() {
            ++messages_received_;
        }

        void IncFailedMessages() {
            ++messages_failed_;
        }

        void IncFailedAck() {
            ++ack_failed_;
        }

    private:
        unique_ptr<Timer> timer_ = make_unique<Timer>();
        size_t messages_received_ = 0;
        size_t messages_failed_ = 0;
        size_t ack_failed_ = 0;
        const size_t id_;
        const unsigned cid_;
    };

public:
    Receiver(boost::asio::io_context& ctx, const Config& conf, unsigned id, size_t consumers)
        : Base(ctx, conf), consumers_{consumers}, id_{id}
    {
    }

protected:

    void ReceiveOne(Channel& ch) {
        ch.consumer_.receiveAsync([this, &ch](pulsar::Result res,
                                  const pulsar::Message& msg) {
            if (res != pulsar::ResultOk) {
                BOOST_LOG_TRIVIAL ( error ) << ch.Name() << ": Failed to receive :" << res;
                ch.IncFailedMessages();
            } else {

                BOOST_LOG_TRIVIAL ( trace ) << ch.Name() << " Received message: " << msg.getMessageId();

                ch.IncOkMessages();
                ch.consumer_.acknowledgeAsync(msg, [&ch] (pulsar::Result res) {
                    if (res != pulsar::ResultOk) {
                        BOOST_LOG_TRIVIAL ( error ) << ch.Name() << ": Failed to ack:" << res;
                        return;
                    }

                    ch.IncFailedAck();
                });

                if (msg.getLength() >= 1) {
                    const auto data = static_cast<const char *>(msg.getData());
                    if (data[0] == 0) {
                        BOOST_LOG_TRIVIAL (debug) << ch.Name() << " has received its final message.";

                        ch.consumer_.closeAsync([this](pulsar::Result) {
                            if (receivers_.size() == ++completed_) {
                                BOOST_LOG_TRIVIAL ( info ) << "All subscribers are done.";

                                result_.duration_ = timer_->elapsedSeconds();

                                for(const auto& r: receivers_) {
                                    result_.ok_messages += r->GetNumMessagesReceived();
                                    result_.failed_messages += r->GetNumMessagesFailed();
                                    result_.aggregated_avg_per_sec += r->GetAvgMessagePerSecond();
                                }

                                result_.avg_per_sec = result_.aggregated_avg_per_sec / receivers_.size();

                                BOOST_LOG_TRIVIAL (info) << "All consumers have finished in "
                                                         << result_.duration_ << " sec.";
                                BOOST_LOG_TRIVIAL (info) << "Messages received: " << result_.ok_messages
                                                         << ", messages failed: " << result_.failed_messages
                                                         << ", avg " << result_.avg_per_sec
                                                         << " messages/sec, aggregated: " << result_.aggregated_avg_per_sec << " messages/sec.";

                                Shutdown();
                            }
                        });

                        return;
                    }
                }
            }

            ch.strand_.post([this, &ch]() {
                ReceiveOne(ch);
            });
        });
    }

    void Run_() override {
        BOOST_LOG_TRIVIAL ( debug ) << "Consumer " << id_ << ": Will subscribe " << conf_.consumers << " times to " << conf_.topic_name;
        size_t next_subscriber = 0;
        for(size_t topic_id = 0; topic_id < consumers_; ++topic_id) {
            ostringstream topic;
            topic << "persistent:/"
                << '/' << conf_.tenant_name
                << '/' << conf_.namespace_name
                << '/' << conf_.topic_name;

            auto topic_str = topic.str();
            auto channel = make_shared<Channel>(id_, ++next_subscriber, ctx_);

            client_->subscribeAsync(topic_str, channel->Name(),
                                   [this, topic_str, channel] (pulsar::Result res, pulsar::Consumer c) {

               if (res != pulsar::ResultOk) {
                   BOOST_LOG_TRIVIAL ( error ) << channel->Name() << ": Failed to subscribe to " << topic_str;
                   BOOST_LOG_TRIVIAL ( error ) << "Calling Shutdwon()";
                   Shutdown();
                   return;
               }

               channel->consumer_ = move(c);
               BOOST_LOG_TRIVIAL ( debug ) << channel->Name()
                                           << " successfully subscribed to "
                                           << topic_str;

               ReceiveOne(*channel);
               receivers_.push_back(channel);
            });
        }
    }

private:
    std::vector<std::shared_ptr<Channel>> receivers_;
    size_t completed_ = 0;
    const size_t consumers_; // Number of consumers for this instance
    const unsigned id_;
};

std::unique_ptr<Base> Base::CreateConsumer(boost::asio::io_context& ctx, const Config& conf,
                                           unsigned cid,
                                           size_t consumers) {
    return make_unique<Receiver>(ctx, conf, cid, consumers);
}
