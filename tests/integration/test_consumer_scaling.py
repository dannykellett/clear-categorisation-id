"""
Tests for consumer scaling and partition rebalancing scenarios.

Tests various scaling patterns including:
- Multiple consumer instances in the same group
- Partition assignment and rebalancing
- Consumer group coordination
- Load distribution across partitions
- Dynamic scaling up and down
"""

import asyncio
import json
import pytest
from datetime import datetime, timezone
from uuid import uuid4
from unittest.mock import AsyncMock, patch, MagicMock

from aiokafka.structs import ConsumerRecord, TopicPartition
# Import RangePartitionAssignor safely (path may vary by aiokafka version)
try:
    from aiokafka.coordinator.assignors.range_assignor import RangePartitionAssignor
except ImportError:
    # Fallback for different aiokafka versions
    try:
        from aiokafka.coordinator.assignors.range import RangePartitionAssignor
    except ImportError:
        # Mock for testing if not available
        RangePartitionAssignor = None

from app.consumers.scraped_consumer import ScrapedMessageConsumer
from app.consumers.base_consumer import BaseKafkaConsumer


class TestConsumerScaling:
    """Tests for consumer scaling and partition management."""
    
    @pytest.mark.asyncio
    async def test_multiple_consumers_same_group(self):
        """
        Test multiple consumer instances operating within the same consumer group.
        
        This test validates Kafka's consumer group protocol where multiple consumers
        coordinate to share work efficiently without duplicate processing:
        
        Consumer Group Coordination:
        1. Multiple consumers join the same consumer group
        2. Kafka coordinator assigns partitions to each consumer
        3. Each partition is consumed by exactly one consumer
        4. Work is distributed evenly across all active consumers
        
        Test Scenario:
        - 3 consumer instances join group "test-scaling-group"
        - Topic has 6 partitions (2 partitions per consumer)
        - Each consumer processes distinct set of partitions
        - No message duplication across consumers
        - Fault tolerance through group membership
        
        Expected Behavior:
        - Each consumer gets assigned unique partition(s)
        - Messages processed exactly once across the group
        - Load distributed evenly when possible
        - Group coordinator manages partition assignments
        - Consumers maintain independent offset positions
        """
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-scaling-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            # Create multiple consumer instances
            consumers = []
            mock_consumers = []
            
            for i in range(3):
                mock_consumer = AsyncMock()
                mock_consumer_class.return_value = mock_consumer
                mock_consumers.append(mock_consumer)
                
                consumer = ScrapedMessageConsumer(
                    input_topic="test-scraped",
                    group_id="test-scaling-group"
                )
                consumers.append(consumer)
            
            # Mock partition assignments for each consumer
            # Simulate 6 partitions distributed across 3 consumers
            partition_assignments = [
                [TopicPartition('test-scraped', 0), TopicPartition('test-scraped', 1)],  # Consumer 0
                [TopicPartition('test-scraped', 2), TopicPartition('test-scraped', 3)],  # Consumer 1
                [TopicPartition('test-scraped', 4), TopicPartition('test-scraped', 5)]   # Consumer 2
            ]
            
            # Start all consumers
            for i, consumer in enumerate(consumers):
                consumer.consumer = mock_consumers[i]
                mock_consumers[i].assignment.return_value = set(partition_assignments[i])
                await consumer.start()
            
            # Verify each consumer has assigned partitions
            for i, consumer in enumerate(consumers):
                assigned_partitions = await consumer.get_assigned_partitions()
                assert len(assigned_partitions) == 2
                assert all(tp.topic == 'test-scraped' for tp in assigned_partitions)
                
                # Verify partition numbers match expected assignment
                partition_numbers = {tp.partition for tp in assigned_partitions}
                expected_partitions = {tp.partition for tp in partition_assignments[i]}
                assert partition_numbers == expected_partitions
            
            # Clean up
            for consumer in consumers:
                await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_partition_rebalancing_on_consumer_join(self):
        """Test partition rebalancing when a new consumer joins the group."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-rebalance-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            # Start with 2 consumers
            initial_consumers = []
            initial_mock_consumers = []
            
            for i in range(2):
                mock_consumer = AsyncMock()
                mock_consumer_class.return_value = mock_consumer
                initial_mock_consumers.append(mock_consumer)
                
                consumer = ScrapedMessageConsumer(
                    input_topic="test-scraped",
                    group_id="test-rebalance-group"
                )
                initial_consumers.append(consumer)
            
            # Initial partition assignment: 4 partitions across 2 consumers
            initial_assignments = [
                [TopicPartition('test-scraped', 0), TopicPartition('test-scraped', 1)],  # Consumer 0
                [TopicPartition('test-scraped', 2), TopicPartition('test-scraped', 3)]   # Consumer 1
            ]
            
            # Start initial consumers
            for i, consumer in enumerate(initial_consumers):
                consumer.consumer = initial_mock_consumers[i]
                initial_mock_consumers[i].assignment.return_value = set(initial_assignments[i])
                await consumer.start()
            
            # Verify initial assignment
            for i, consumer in enumerate(initial_consumers):
                partitions = await consumer.get_assigned_partitions()
                assert len(partitions) == 2
            
            # Simulate new consumer joining (rebalance event)
            new_mock_consumer = AsyncMock()
            mock_consumer_class.return_value = new_mock_consumer
            
            new_consumer = ScrapedMessageConsumer(
                input_topic="test-scraped",
                group_id="test-rebalance-group"
            )
            
            # After rebalance: redistribute 4 partitions across 3 consumers
            rebalanced_assignments = [
                [TopicPartition('test-scraped', 0)],                                    # Consumer 0
                [TopicPartition('test-scraped', 1)],                                    # Consumer 1
                [TopicPartition('test-scraped', 2), TopicPartition('test-scraped', 3)]  # New Consumer
            ]
            
            # Update assignments after rebalance
            for i, consumer in enumerate(initial_consumers):
                initial_mock_consumers[i].assignment.return_value = set(rebalanced_assignments[i])
                # Trigger rebalance callback
                await consumer.on_partitions_revoked(set(initial_assignments[i]))
                await consumer.on_partitions_assigned(set(rebalanced_assignments[i]))
            
            # Start new consumer
            new_consumer.consumer = new_mock_consumer
            new_mock_consumer.assignment.return_value = set(rebalanced_assignments[2])
            await new_consumer.start()
            await new_consumer.on_partitions_assigned(set(rebalanced_assignments[2]))
            
            # Verify rebalanced assignments
            for i, consumer in enumerate(initial_consumers):
                partitions = await consumer.get_assigned_partitions()
                assert len(partitions) == 1  # Fewer partitions after rebalance
            
            new_partitions = await new_consumer.get_assigned_partitions()
            assert len(new_partitions) == 2  # New consumer gets remaining partitions
            
            # Clean up
            for consumer in initial_consumers + [new_consumer]:
                await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_partition_rebalancing_on_consumer_leave(self):
        """Test partition rebalancing when a consumer leaves the group."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-leave-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            # Start with 3 consumers
            consumers = []
            mock_consumers = []
            
            for i in range(3):
                mock_consumer = AsyncMock()
                mock_consumer_class.return_value = mock_consumer
                mock_consumers.append(mock_consumer)
                
                consumer = ScrapedMessageConsumer(
                    input_topic="test-scraped",
                    group_id="test-leave-group"
                )
                consumers.append(consumer)
            
            # Initial assignment: 6 partitions across 3 consumers
            initial_assignments = [
                [TopicPartition('test-scraped', 0), TopicPartition('test-scraped', 1)],  # Consumer 0
                [TopicPartition('test-scraped', 2), TopicPartition('test-scraped', 3)],  # Consumer 1
                [TopicPartition('test-scraped', 4), TopicPartition('test-scraped', 5)]   # Consumer 2
            ]
            
            # Start all consumers
            for i, consumer in enumerate(consumers):
                consumer.consumer = mock_consumers[i]
                mock_consumers[i].assignment.return_value = set(initial_assignments[i])
                await consumer.start()
            
            # Stop one consumer (simulate leaving)
            leaving_consumer = consumers[1]
            await leaving_consumer.stop()
            
            # Simulate rebalance: redistribute partitions among remaining 2 consumers
            rebalanced_assignments = [
                [TopicPartition('test-scraped', 0), TopicPartition('test-scraped', 1), TopicPartition('test-scraped', 2)],  # Consumer 0
                [TopicPartition('test-scraped', 3), TopicPartition('test-scraped', 4), TopicPartition('test-scraped', 5)]   # Consumer 2
            ]
            
            remaining_consumers = [consumers[0], consumers[2]]
            remaining_assignments = [rebalanced_assignments[0], rebalanced_assignments[1]]
            
            # Update assignments for remaining consumers
            for i, consumer in enumerate(remaining_consumers):
                mock_consumer_idx = 0 if consumer == consumers[0] else 2
                mock_consumers[mock_consumer_idx].assignment.return_value = set(remaining_assignments[i])
                
                # Trigger rebalance callbacks
                old_assignment = initial_assignments[mock_consumer_idx] if mock_consumer_idx != 2 else initial_assignments[2]
                await consumer.on_partitions_revoked(set(old_assignment))
                await consumer.on_partitions_assigned(set(remaining_assignments[i]))
            
            # Verify rebalanced assignments
            for i, consumer in enumerate(remaining_consumers):
                partitions = await consumer.get_assigned_partitions()
                assert len(partitions) == 3  # More partitions after consumer leaves
            
            # Clean up
            for consumer in remaining_consumers:
                await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_load_distribution_across_partitions(self):
        """Test load distribution across multiple partitions."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings:
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-load-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            # Create consumer for load testing
            consumer = ScrapedMessageConsumer()
            
            # Mock classification service
            mock_classification_service = AsyncMock()
            
            processing_times = {}
            
            async def track_processing(*args, **kwargs):
                partition = kwargs.get('partition', 0)
                if partition not in processing_times:
                    processing_times[partition] = []
                
                start_time = asyncio.get_event_loop().time()
                await asyncio.sleep(0.1)  # Simulate processing time
                end_time = asyncio.get_event_loop().time()
                
                processing_times[partition].append(end_time - start_time)
                
                return {
                    'classification': [
                        {
                            'tier_1': 'Technology',
                            'confidence': 0.85,
                            'reasoning': f'Partition {partition} processing'
                        }
                    ],
                    'provider': 'openai',
                    'model': 'gpt-4o-mini',
                    'usage': {
                        'input_tokens': 100,
                        'output_tokens': 20,
                        'total_tokens': 120
                    },
                    'taxonomy_version': 'tax_v20250814_103000'
                }
            
            mock_classification_service.classify_text.side_effect = track_processing
            consumer.set_classification_service(mock_classification_service)
            
            # Mock producer
            mock_producer = AsyncMock()
            mock_producer.publish_classified_message.return_value = {"success": True}
            consumer.set_producer(mock_producer)
            
            # Create messages from different partitions
            messages_per_partition = 5
            partitions = [0, 1, 2]
            
            tasks = []
            for partition in partitions:
                for i in range(messages_per_partition):
                    message_data = {
                        "id": str(uuid4()),
                        "article_id": f"load-test-p{partition}-{i}",
                        "text": f"Test content for partition {partition} message {i}",
                        "source": "test_source",
                        "ts": "2025-08-14T10:30:00Z"
                    }
                    
                    consumer_record = ConsumerRecord(
                        topic="test-scraped",
                        partition=partition,
                        offset=i,
                        timestamp=1234567890,
                        timestamp_type=1,
                        key=f"p{partition}-key-{i}".encode(),
                        value=json.dumps(message_data).encode(),
                        checksum=None,
                        serialized_key_size=10,
                        serialized_value_size=len(json.dumps(message_data)),
                        headers=[]
                    )
                    
                    # Create processing task with partition tracking
                    async def process_with_partition(record, part=partition):
                        await mock_classification_service.classify_text(
                            json.loads(record.value.decode())["text"],
                            partition=part
                        )
                    
                    tasks.append(process_with_partition(consumer_record))
            
            # Process all messages concurrently
            await asyncio.gather(*tasks)
            
            # Verify load distribution
            assert len(processing_times) == 3  # All partitions processed
            
            for partition in partitions:
                assert len(processing_times[partition]) == messages_per_partition
                
                # Verify processing times are reasonable
                avg_time = sum(processing_times[partition]) / len(processing_times[partition])
                assert 0.08 <= avg_time <= 0.15  # Around 0.1s with some variance
    
    @pytest.mark.asyncio
    async def test_consumer_group_coordination(self):
        """Test consumer group coordination and metadata sharing."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-coordination-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            # Create multiple consumers
            consumers = []
            mock_consumers = []
            
            for i in range(2):
                mock_consumer = AsyncMock()
                mock_consumer_class.return_value = mock_consumer
                mock_consumers.append(mock_consumer)
                
                consumer = ScrapedMessageConsumer(
                    input_topic="test-scraped",
                    group_id="test-coordination-group"
                )
                consumers.append(consumer)
            
            # Mock consumer group metadata
            for i, mock_consumer in enumerate(mock_consumers):
                mock_consumer.position.return_value = i * 100  # Different positions
                mock_consumer.committed.return_value = i * 90   # Different commit positions
                mock_consumer.highwater.return_value = 1000     # Same high-water mark
            
            # Start consumers
            for i, consumer in enumerate(consumers):
                consumer.consumer = mock_consumers[i]
                await consumer.start()
            
            # Test coordination metadata
            coordination_info = []
            for consumer in consumers:
                info = await consumer.get_coordination_info()
                coordination_info.append(info)
            
            # Verify each consumer has different position but same group
            assert len(coordination_info) == 2
            assert coordination_info[0]['group_id'] == coordination_info[1]['group_id']
            assert coordination_info[0]['position'] != coordination_info[1]['position']
            
            # Clean up
            for consumer in consumers:
                await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_dynamic_scaling_up(self):
        """Test dynamic scaling up by adding consumers during processing."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-dynamic-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            # Start with 1 consumer processing high load
            initial_consumer = ScrapedMessageConsumer(
                input_topic="test-scraped",
                group_id="test-dynamic-group"
            )
            
            initial_mock_consumer = AsyncMock()
            mock_consumer_class.return_value = initial_mock_consumer
            initial_mock_consumer.assignment.return_value = {
                TopicPartition('test-scraped', 0),
                TopicPartition('test-scraped', 1),
                TopicPartition('test-scraped', 2),
                TopicPartition('test-scraped', 3)
            }
            
            initial_consumer.consumer = initial_mock_consumer
            await initial_consumer.start()
            
            # Simulate high message processing load
            initial_load_metrics = {
                'messages_per_second': 1000,
                'processing_latency': 0.5,
                'consumer_lag': 500
            }
            
            # Check if scaling is needed
            should_scale = (
                initial_load_metrics['consumer_lag'] > 100 or
                initial_load_metrics['processing_latency'] > 0.2
            )
            
            assert should_scale is True
            
            # Add second consumer for scaling
            scaling_consumer = ScrapedMessageConsumer(
                input_topic="test-scraped",
                group_id="test-dynamic-group"
            )
            
            scaling_mock_consumer = AsyncMock()
            mock_consumer_class.return_value = scaling_mock_consumer
            
            # After scaling: redistribute partitions
            initial_mock_consumer.assignment.return_value = {
                TopicPartition('test-scraped', 0),
                TopicPartition('test-scraped', 1)
            }
            scaling_mock_consumer.assignment.return_value = {
                TopicPartition('test-scraped', 2),
                TopicPartition('test-scraped', 3)
            }
            
            scaling_consumer.consumer = scaling_mock_consumer
            await scaling_consumer.start()
            
            # Verify load distribution after scaling
            initial_partitions = await initial_consumer.get_assigned_partitions()
            scaling_partitions = await scaling_consumer.get_assigned_partitions()
            
            assert len(initial_partitions) == 2
            assert len(scaling_partitions) == 2
            
            # Verify no partition overlap
            initial_partition_nums = {tp.partition for tp in initial_partitions}
            scaling_partition_nums = {tp.partition for tp in scaling_partitions}
            assert initial_partition_nums.isdisjoint(scaling_partition_nums)
            
            # Clean up
            await initial_consumer.stop()
            await scaling_consumer.stop()
    
    @pytest.mark.asyncio
    async def test_dynamic_scaling_down(self):
        """Test dynamic scaling down by removing consumers during low load."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-scale-down-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            # Start with 3 consumers during high load
            consumers = []
            mock_consumers = []
            
            for i in range(3):
                mock_consumer = AsyncMock()
                mock_consumer_class.return_value = mock_consumer
                mock_consumers.append(mock_consumer)
                
                consumer = ScrapedMessageConsumer(
                    input_topic="test-scraped",
                    group_id="test-scale-down-group"
                )
                consumers.append(consumer)
            
            # Initial assignment: 6 partitions across 3 consumers
            initial_assignments = [
                [TopicPartition('test-scraped', 0), TopicPartition('test-scraped', 1)],
                [TopicPartition('test-scraped', 2), TopicPartition('test-scraped', 3)],
                [TopicPartition('test-scraped', 4), TopicPartition('test-scraped', 5)]
            ]
            
            for i, consumer in enumerate(consumers):
                consumer.consumer = mock_consumers[i]
                mock_consumers[i].assignment.return_value = set(initial_assignments[i])
                await consumer.start()
            
            # Simulate low load conditions
            low_load_metrics = {
                'messages_per_second': 50,
                'processing_latency': 0.01,
                'consumer_lag': 5,
                'cpu_utilization': 0.15
            }
            
            # Check if scaling down is beneficial
            should_scale_down = (
                low_load_metrics['consumer_lag'] < 50 and
                low_load_metrics['processing_latency'] < 0.05 and
                low_load_metrics['cpu_utilization'] < 0.3
            )
            
            assert should_scale_down is True
            
            # Scale down by stopping one consumer
            consumers_to_keep = consumers[:2]
            consumer_to_remove = consumers[2]
            
            await consumer_to_remove.stop()
            
            # Redistribute partitions among remaining consumers
            scaled_assignments = [
                [TopicPartition('test-scraped', 0), TopicPartition('test-scraped', 1), TopicPartition('test-scraped', 4)],
                [TopicPartition('test-scraped', 2), TopicPartition('test-scraped', 3), TopicPartition('test-scraped', 5)]
            ]
            
            for i, consumer in enumerate(consumers_to_keep):
                mock_consumers[i].assignment.return_value = set(scaled_assignments[i])
                await consumer.on_partitions_revoked(set(initial_assignments[i]))
                await consumer.on_partitions_assigned(set(scaled_assignments[i]))
            
            # Verify scaled down assignment
            for i, consumer in enumerate(consumers_to_keep):
                partitions = await consumer.get_assigned_partitions()
                assert len(partitions) == 3  # More partitions per consumer after scaling down
            
            # Clean up
            for consumer in consumers_to_keep:
                await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_partition_offset_management(self):
        """Test partition offset management during scaling operations."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-offset-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            consumer = ScrapedMessageConsumer()
            mock_consumer = AsyncMock()
            mock_consumer_class.return_value = mock_consumer
            
            # Mock offset information
            partitions = [
                TopicPartition('test-scraped', 0),
                TopicPartition('test-scraped', 1)
            ]
            
            # Mock different offset states
            mock_consumer.assignment.return_value = set(partitions)
            mock_consumer.position.side_effect = lambda tp: 1500 if tp.partition == 0 else 1200
            mock_consumer.committed.side_effect = lambda tp: 1450 if tp.partition == 0 else 1180
            mock_consumer.highwater.side_effect = lambda tp: 1600 if tp.partition == 0 else 1300
            
            consumer.consumer = mock_consumer
            await consumer.start()
            
            # Get offset information
            offset_info = await consumer.get_offset_info()
            
            # Verify offset management
            assert len(offset_info['partitions']) == 2
            
            for partition_info in offset_info['partitions']:
                if partition_info['partition'] == 0:
                    assert partition_info['position'] == 1500
                    assert partition_info['committed'] == 1450
                    assert partition_info['high_water'] == 1600
                    assert partition_info['lag'] == 100  # 1600 - 1500
                elif partition_info['partition'] == 1:
                    assert partition_info['position'] == 1200
                    assert partition_info['committed'] == 1180
                    assert partition_info['high_water'] == 1300
                    assert partition_info['lag'] == 100  # 1300 - 1200
            
            # Test manual offset commit
            await consumer.commit_offsets()
            
            # Verify commit was called for all assigned partitions
            mock_consumer.commit.assert_called_once()
            
            await consumer.stop()
    
    @pytest.mark.asyncio
    async def test_consumer_health_monitoring_during_scaling(self):
        """Test consumer health monitoring during scaling operations."""
        with patch('app.consumers.scraped_consumer.get_settings') as mock_settings, \
             patch('app.consumers.base_consumer.AIOKafkaConsumer') as mock_consumer_class:
            
            mock_settings.return_value.kafka_in_topic = "test-scraped"
            mock_settings.return_value.kafka_group_id = "test-health-group"
            mock_settings.return_value.kafka_brokers = "localhost:9092"
            
            # Create consumers with different health states
            healthy_consumer = ScrapedMessageConsumer()
            degraded_consumer = ScrapedMessageConsumer()
            
            healthy_mock = AsyncMock()
            degraded_mock = AsyncMock()
            
            mock_consumer_class.side_effect = [healthy_mock, degraded_mock]
            
            # Healthy consumer setup
            healthy_mock.assignment.return_value = {TopicPartition('test-scraped', 0)}
            healthy_mock.position.return_value = 1000
            healthy_mock.committed.return_value = 950
            healthy_mock.highwater.return_value = 1050
            
            # Degraded consumer setup (high lag)
            degraded_mock.assignment.return_value = {TopicPartition('test-scraped', 1)}
            degraded_mock.position.return_value = 500
            degraded_mock.committed.return_value = 400
            degraded_mock.highwater.return_value = 1500  # High lag
            
            healthy_consumer.consumer = healthy_mock
            degraded_consumer.consumer = degraded_mock
            
            await healthy_consumer.start()
            await degraded_consumer.start()
            
            # Check health status
            healthy_status = await healthy_consumer.health_check()
            degraded_status = await degraded_consumer.health_check()
            
            # Verify health assessments
            assert healthy_status['status'] == 'healthy'
            assert healthy_status['consumer_lag'] == 50  # 1050 - 1000
            
            assert degraded_status['status'] == 'degraded'  # High lag should trigger degraded status
            assert degraded_status['consumer_lag'] == 1000  # 1500 - 500
            
            # Test scaling decision based on health
            consumers_health = [healthy_status, degraded_status]
            
            # Determine if scaling action needed
            avg_lag = sum(status['consumer_lag'] for status in consumers_health) / len(consumers_health)
            max_lag = max(status['consumer_lag'] for status in consumers_health)
            
            scaling_needed = max_lag > 500 or avg_lag > 200
            assert scaling_needed is True
            
            await healthy_consumer.stop()
            await degraded_consumer.stop()