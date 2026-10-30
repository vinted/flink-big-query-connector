package com.vinted.flink.bigquery.util;

import com.vinted.flink.bigquery.model.Rows;
import com.vinted.flink.bigquery.process.StreamState;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.extension.*;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class FlinkTest implements AfterAllCallback, AfterEachCallback, BeforeEachCallback, ParameterResolver {
    private Path tempDir;

    private int defaultParallelism = 1;

    {
        try {
            tempDir = Files.createTempDirectory("flink-checkpoints");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Configuration config = new Configuration();

    {
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, tempDir.toUri().toString());
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, tempDir.toUri().toString());
        config.set(CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

    }

    private MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(defaultParallelism)
            .setNumberTaskManagers(1)
            .setConfiguration(config)
            .build());

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        FileUtils.deleteDirectory(tempDir.toFile());
    }
    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        flinkCluster.after();
        MockClock.reset();
        MockJsonClientProvider.reset();
        MockProtoClientProvider.reset();
        MockAsyncProtoClientProvider.reset();
        ProcessFunctionWithError.clear();
        TestSink.clear();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        flinkCluster.before();
        MockClock.reset();
        MockJsonClientProvider.reset();
        MockProtoClientProvider.reset();
        MockAsyncProtoClientProvider.reset();
        ProcessFunctionWithError.clear();
        TestSink.clear();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.isAnnotated(FlinkParam.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Class<?> type = parameterContext.getParameter().getType();
        if (MockClock.class.equals(type)) {
            return new MockClock();
        }
        if (MockJsonClientProvider.class.equals(type)) {
            return new MockJsonClientProvider<>();
        }

        if (MockProtoClientProvider.class.equals(type)) {
            return new MockProtoClientProvider();
        }

        if (MockAsyncProtoClientProvider.class.equals(type)) {
            return new MockAsyncProtoClientProvider();
        }

        if (PipelineRunner.class.equals(type)) {
            return new PipelineRunner();
        }
        throw new ParameterResolutionException("No random generator implemented for " + type);
    }
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER})
    public @interface FlinkParam {

    }


    public class PipelineRunner {
        private int defaultParallelism = 1;
        private int retryCount = 1;

        private boolean error = false;
        private int errorAfter = 0;

        public PipelineRunner withRetryCount(int count) {
            this.retryCount = count;
            return this;
        }

        public PipelineRunner withDefaultParallelism(int value) {
            this.defaultParallelism = value;
            return this;
        }

        public PipelineRunner withErrorAfter(int records) {
            this.error = true;
            this.errorAfter = records;
            return this;
        }


        public <T> List<T> run(Function<StreamExecutionEnvironment, DataStream<T>> execute) throws Exception {
            var env = getExecutionEnvironment();
            var testSink = new TestSink<T>();
            var stream = error ? execute.apply(env).process(new ProcessFunctionWithError<>(errorAfter)) : execute.apply(env);
            stream.sinkTo(testSink);
            var result = env.execute();
            return testSink.getResults(result);
        }

        private StreamExecutionEnvironment getExecutionEnvironment() {
            var restartConfig = new Configuration();
            restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, retryCount);
            restartConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(5));

            var env = StreamExecutionEnvironment.getExecutionEnvironment(restartConfig);
            env.setParallelism(defaultParallelism);
            env.enableCheckpointing(10);
            env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10);
            env.getCheckpointConfig().enableUnalignedCheckpoints();
            env.getCheckpointConfig().setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
            env.configure(config);

            var serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
            serializerConfig.registerKryoType(StreamState.class);
            serializerConfig.registerKryoType(Rows.class);
            return env;
        }

        public <T> void runWithCustomSink(Function<StreamExecutionEnvironment, DataStreamSink<T>> execute) throws Exception {
            var env = getExecutionEnvironment();
            execute.apply(env);
            env.execute();
        }
    }
}
