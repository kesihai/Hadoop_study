package studyDisruptor;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import java.nio.ByteBuffer;

public class LongEventMain {
  public static void main(String[] args) throws InterruptedException {
    int bufferSize = 1024;
    Disruptor<LongEvent> disruptor = new Disruptor<>(LongEvent::new,
        bufferSize, DaemonThreadFactory.INSTANCE);
    disruptor.handleEventsWith(new LongEventHandler());
    disruptor.start();

    RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();
    ByteBuffer bb = ByteBuffer.allocate(8);
    for (long l = 0; true; l++) {
      bb.putLong(0, l);
      ringBuffer.publishEvent(new EventTranslator<LongEvent>() {
        @Override
        public void translateTo(LongEvent event, long sequence) {
          event.set(bb.getLong(0));
        }
      });
      Thread.sleep(1000);
    }
  }

}
