package io.github.dbstarll.flume.channel;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.AbstractChannelSelector;

import java.util.Collections;
import java.util.List;

public class HashChannelSelector extends AbstractChannelSelector {
    private final List<Channel> emptyList = Collections.emptyList();

    @Override
    public void configure(final Context context) {
    }

    @Override
    public List<Channel> getRequiredChannels(final Event event) {
        final List<Channel> channels = getAllChannels();
        final Channel channel = channels.get(event.getBody().hashCode() % channels.size());
        return Collections.singletonList(channel);
    }

    @Override
    public List<Channel> getOptionalChannels(final Event event) {
        return emptyList;
    }
}
