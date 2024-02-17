package domain

import (
	"errors"
	"time"
)

type ActorSpawner struct {
	actorSpawningDao     ActorSpawningDao
	spawnedActors        map[ActorId]Actor
	cachedPartitionTable map[PartitionName]*FreshPartition

	partitionCacheValidInterval time.Duration
	maxActorsPerShard           int
	minNewShardsCount           int
}

func NewActorSpawner(actorSpawningDao ActorSpawningDao, partitionCacheValidInterval time.Duration, maxActorsPerShard int, minNewShardsCount int) *ActorSpawner {
	return &ActorSpawner{
		actorSpawningDao:            actorSpawningDao,
		spawnedActors:               make(map[ActorId]Actor),
		cachedPartitionTable:        make(map[PartitionName]*FreshPartition),
		partitionCacheValidInterval: partitionCacheValidInterval,
		maxActorsPerShard:           maxActorsPerShard,
		minNewShardsCount:           minNewShardsCount,
	}
}

func (s *ActorSpawner) Init(executionContext ExecutionContext) {
	s.actorSpawningDao = executionContext.actorSpawningDao
	s.spawnedActors = make(map[ActorId]Actor)
	s.cachedPartitionTable = make(map[PartitionName]*FreshPartition)
	s.partitionCacheValidInterval = executionContext.actorSpawnerConfig.PartitionCacheValidInterval
	s.maxActorsPerShard = executionContext.actorSpawnerConfig.MaxActorsPerShard
	s.minNewShardsCount = executionContext.actorSpawnerConfig.MinNewShardsCount
	executionContext.actorManager.AddSpawningActorsCollector(s)
}

func (s *ActorSpawner) Spawn(newActor Actor, partitionName string, instanceId string) (ActorId, error) {
	shardId, err := s.AssignShard(partitionName)
	if err != nil {
		return ActorId{}, err
	}

	actorId := ActorId{
		InstanceId: instanceId,
		PhyPartitionId: PhysicalPartitionId{
			PartitionName:         partitionName,
			PhysicalPartitionName: string(shardId),
		},
	}
	newActor.SetId(actorId)
	s.spawnedActors[actorId] = newActor

	return actorId, nil
}

func (s *ActorSpawner) GetSpawningActors() []Actor {
	var spawningActors []Actor

	for _, actor := range s.spawnedActors {
		spawningActors = append(spawningActors, actor)
	}

	s.spawnedActors = make(map[ActorId]Actor)

	return spawningActors
}

func (s *ActorSpawner) AssignShard(partitionName string) (ShardId, error) {
	partition, isPartitionCached := s.cachedPartitionTable[PartitionName(partitionName)]
	if !isPartitionCached || partition.IsExpired(s.partitionCacheValidInterval) {
		var err error
		partition, err = s.actorSpawningDao.FetchPartition(PartitionName(partitionName))

		if err != nil {
			return "", err
		}

		s.cachedPartitionTable[partition.id] = partition
	}

	lighterShard, _ := partition.GetLighterShard(s.maxActorsPerShard)
	if lighterShard.GetRemainingActorPlacesCount(s.maxActorsPerShard) >= 1 && lighterShard.id != "0" { //shard 0 is reserved for the aggregator
		err := s.actorSpawningDao.IncrementActorsCount(partition.id, lighterShard.id)
		if err != nil {
			return "", err
		}
		return lighterShard.id, nil
	} else {
		newShards, err := s.actorSpawningDao.AddShards(partition.id, s.minNewShardsCount)
		if err != nil {
			return "", err
		}
		if len(newShards) == 0 {
			return "", errors.New("could not add any shard to dynamodb")
		}

		s.cachedPartitionTable[partition.id] = NewFreshPartition(partition.id, newShards)

		return s.AssignShard(string(partition.id))
	}
}

func (s *ActorSpawner) AddPartition(partitionName string) error {
	return s.actorSpawningDao.AddPartition(partitionName)
}

type PartitionName string
type FreshPartition struct {
	id        PartitionName
	shards    []*Shard
	fetchTime time.Time
}

func NewFreshPartition(id PartitionName, shards []*Shard) *FreshPartition {
	return &FreshPartition{
		id:        id,
		shards:    shards,
		fetchTime: time.Now(),
	}
}

func (p *FreshPartition) IsExpired(maxCacheValidityInterval time.Duration) bool {
	return time.Now().After(p.fetchTime.Add(maxCacheValidityInterval))
}

func (p *FreshPartition) GetLighterShard(maximumActorsPerShard int) (*Shard, bool) {
	var lighterShard *Shard
	for _, shard := range p.shards {
		if lighterShard == nil || shard.GetRemainingActorPlacesCount(maximumActorsPerShard) > lighterShard.GetRemainingActorPlacesCount(maximumActorsPerShard) {
			lighterShard = shard
		}
	}

	return lighterShard, lighterShard != nil
}

type ShardId string
type Shard struct {
	id                   ShardId
	allocatedActorsCount int
}

func NewShard(id ShardId, allocatedActorsCount int) *Shard {
	return &Shard{id: id, allocatedActorsCount: allocatedActorsCount}
}

func (s *Shard) GetRemainingActorPlacesCount(maximumActorsPerShard int) int {
	return maximumActorsPerShard - s.allocatedActorsCount
}
