import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;


enum MessageType{
    STATUS,
    CSV,
    TRIGGER;
}
class Message{
    private String message;
    private MessageType messageType;

    public Message(String message, MessageType messageType) {
        this.message = message;
        this.messageType = messageType;
    }

    public String getMessage() {
        return message;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    @Override
    public String toString() {
        return "Message{" +
                "message='" + message + '\'' +
                ", messageType=" + messageType +
                '}';
    }
}
public class DummyStream {
    public void runStream(){


        Graph<FanOutShape2<String,Message,Message>, NotUsed> readerGraphDSL = GraphDSL.create(
                builder -> {
                    UniformFanInShape<Message,Message> merge = builder.add(Merge.create(2));
                    FanInShape2<String, Message, String> zipWith = builder.add(ZipWith.create((message,trigger)->message));
                    UniformFanOutShape<Message,Message> broadcast = builder.add(Broadcast.create(3));

                    FlowShape<String,Message> readerFlow = builder.add(Flow.of(String.class).mapConcat(
                            chunkWiseQuery -> {
                                List<Message> outMessages = new ArrayList<>();
                                outMessages.add(new Message("THIS IS STATUS",MessageType.STATUS));
                                outMessages.add(new Message(chunkWiseQuery, MessageType.CSV));
//                                if(!chunkWiseQuery.equals("500"))
                                    outMessages.add(new Message("THIS IS TRIGGER", MessageType.TRIGGER));
                                return outMessages;
                            }
                    ));

                    FlowShape<Message, Message> filterStatusFlow = builder.add(Flow.of(Message.class).filter(message -> message.getMessageType() == MessageType.STATUS));
                    FlowShape<Message, Message> filterCSVFlow = builder.add(Flow.of(Message.class).filter(message -> message.getMessageType() == MessageType.CSV));
                    FlowShape<Message, Message> filterTriggerFlow = builder.add(Flow.of(Message.class).filter(message -> message.getMessageType() == MessageType.TRIGGER));

                    SourceShape<Message> sourceShape = builder.add(Source.from(List.of(new Message("",MessageType.TRIGGER))));

                    builder.from(sourceShape).toInlet(merge.in(0));
                    builder.from(merge.out()).toInlet(zipWith.in1());

                    builder.from(zipWith.out()).via(readerFlow).toInlet(broadcast.in());
                    builder.from(broadcast.out(0)).toInlet(filterStatusFlow.in());
                    builder.from(broadcast.out(1)).toInlet(filterCSVFlow.in());
                    builder.from(broadcast.out(2)).via(filterTriggerFlow).toInlet(merge.in(1));

                    return new FanOutShape2(zipWith.in0(),filterStatusFlow.out(),filterCSVFlow.out());
                }
        );

        Source<String, NotUsed> source = Source.range(0,5).map(x->x.toString());
        Sink<Message, CompletionStage<Done>> sink = Sink.foreach(System.out::println);

        RunnableGraph<CompletionStage<Done>> g = RunnableGraph.fromGraph(
                GraphDSL.create(
                        sink,
                        (builder, out)->{

                            UniformFanInShape<Message, Message> mergeSink = builder.add(Merge.create(2));
                            FlowShape<Message,Message> writerFlowShape = builder.add(Flow.of(Message.class).map(csv->{
                                System.out.println("WRITING:"+csv.getMessage());
                                return new Message("DONE WRITING",MessageType.STATUS);
                            }));

                            FanOutShape2<String,Message,Message> readerGraph = builder.add(readerGraphDSL);

                            builder.from(builder.add(source)).toInlet(readerGraph.in());
                            builder.from(readerGraph.out0()).toInlet(mergeSink.in(0));
                            builder.from(readerGraph.out1()).via(writerFlowShape).toInlet(mergeSink.in(1));
                            builder.from(mergeSink).to(out);

                            return ClosedShape.getInstance();
                        }
                )
        );

        g.run(ActorSystem.create());



    }

    public static void main(String[] args) {
        new DummyStream().runStream();
    }
}
