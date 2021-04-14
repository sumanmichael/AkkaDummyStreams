import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletionStage;

public class TestParallelism {
    Logger log  = LogManager.getLogger(getClass());
    public Graph<FlowShape<String,String>, NotUsed> buildGraphModule(int flowId, long sleepDuration){
        return GraphDSL.create(
                builder -> {
                    FlowShape<String,String> flowShape = builder.add(Flow.of(String.class).map(x->{
                        log.error("[R:"+flowId+"]"+x);
                        if(sleepDuration>0){
                            BigInteger b = new BigInteger(3000, new Random());
                            BigInteger p = b.nextProbablePrime();
                            log.error(p.toString());
                        }
                        log.error("[S:"+flowId+"]"+x);
                        return x;
                    }));
                    return flowShape;
                }
        );
    }
    public Graph<FlowShape<String,String>, NotUsed> buildGraphModule2(int flowId, long sleepDuration){
        return GraphDSL.create(
                builder -> {
                    FlowShape<String,String> flowShape = builder.add(Flow.of(String.class).map(x->{
                        log.error("[R:"+flowId+"]"+x);
                        if(sleepDuration>0){
                            BigInteger b = new BigInteger(3000, new Random());
                            BigInteger p = b.nextProbablePrime();
                            log.error(p.toString());
                        }
                        log.error("[S:"+flowId+"]"+x);
                        return x;
                    }));
                    return flowShape;
                }
        );
    }

    public void runStream() throws  Throwable{



        ActorSystem actorSystem = ActorSystem.create();

        Source<String, NotUsed> source = Source.range(0,1000,100).map(Objects::toString);
        Sink<String, CompletionStage<Done>> sink = Sink.foreach(System.out::println);

        RunnableGraph<CompletionStage<Done>> g = RunnableGraph.fromGraph(
                GraphDSL.create(
                        sink,
                        (builder,out)->{

                            int N = 2;
                            UniformFanOutShape<String, String> balance = builder.add(Balance.create(N));
                            UniformFanInShape<String, String> merge = builder.add(Merge.create(N));
                            SourceShape<String> sourceShape = builder.add(source);

                            builder.from(sourceShape).viaFanOut(balance);
                            for (int i = 0; i < N; i++) {
                                if(i==0){
                                    builder.from(balance).via(builder.add(buildGraphModule2(i,100))).viaFanIn(merge);
                                }else{
                                    builder.from(balance).via(builder.add(buildGraphModule2(i,0))).viaFanIn(merge);

                                }
                            }
                            builder.from(merge).to(out);
                            return ClosedShape.getInstance();
                        }
                )
        );

        g.run(actorSystem);

    }

    public static void main(String[] args) throws Throwable{
        new TestParallelism().runStream();
    }
}
