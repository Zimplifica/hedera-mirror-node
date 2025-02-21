package com.hedera.mirror.importer.repository;

/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 - 2021 Hedera Hashgraph, LLC
 * ​
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ‍
 */

import static com.hedera.mirror.importer.config.CacheConfiguration.EXPIRE_AFTER_30M;

import java.util.Optional;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.transaction.annotation.Transactional;

import com.hedera.mirror.importer.domain.Entity;
import com.hedera.mirror.importer.domain.EntityId;

@Transactional
public interface EntityRepository extends PagingAndSortingRepository<Entity, Long> {

    @Cacheable(cacheNames = "entity", cacheManager = EXPIRE_AFTER_30M, key = "{#p0}")
    @Override
    Optional<Entity> findById(Long id);

    @CachePut(cacheNames = "entity", cacheManager = EXPIRE_AFTER_30M, key = "{#p0.id}")
    @Override
    <S extends Entity> S save(S entity);

    @Modifying
    @Query(value = "insert into entity (id, shard, realm, num, type) " +
            "values (?1, ?2, ?3, ?4, ?5) on conflict do nothing", nativeQuery = true)
    void insertEntityId(long id, long shard, long realm, long num, int type);

    default void insertEntityId(EntityId entityId) {
        insertEntityId(entityId.getId(), entityId.getShardNum(), entityId.getRealmNum(),
                entityId.getEntityNum(), entityId.getType());
    }
}
