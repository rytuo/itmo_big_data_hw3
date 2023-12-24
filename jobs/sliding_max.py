from pyflink.common import Time
from pyflink.common.types import Row
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows

from conf.configure_checkpoints import configure_checkpoints


def create_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Set the parallelism to be one to make sure that all data including fired timer and normal data
    # are processed by the same worker and the collected result would be in order which is good for
    # assertion.
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    configure_checkpoints(env)

    type_info: RowTypeInfo = Types.ROW_NAMED(['device_id', 'temperature', 'execution_time'],
                                             [Types.LONG(), Types.DOUBLE(), Types.INT()])

    json_row_deserialization_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()
    json_row_serialization_schema = JsonRowSerializationSchema.builder().with_type_info(type_info).build()
    source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('itmo2023') \
        .set_group_id('window-1') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_row_deserialization_schema) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic('itmo2023-sliding-max')
                .set_value_serialization_schema(json_row_serialization_schema)
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    def max_device_temperature_reducer(v1, v2):
        return Row(
            device_id=v1['device_id'],
            temperature=max(v1['temperature'], v2['temperature']),
            execution_time=(v1['execution_time'] + v2['execution_time']) / 2,
        )

    # device max temperature with tumbling window
    ds \
        .key_by(lambda x: x['device_id']) \
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))) \
        .reduce(
            max_device_temperature_reducer,
            output_type=type_info,
        ) \
        .sink_to(sink)

    env.execute_async("Sliding window preprocessing")


if __name__ == '__main__':
    create_job()
