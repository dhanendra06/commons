package io.mosip.kernel.vidgenerator.service.impl;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import io.mosip.kernel.core.authmanager.authadapter.spi.VertxAuthenticationProvider;
import io.mosip.kernel.core.util.DateUtils;
import io.mosip.kernel.vidgenerator.constant.VIDGeneratorErrorCode;
import io.mosip.kernel.vidgenerator.constant.VidLifecycleStatus;
import io.mosip.kernel.vidgenerator.dto.VidFetchResponseDto;
import io.mosip.kernel.vidgenerator.entity.VidAssignedEntity;
import io.mosip.kernel.vidgenerator.entity.VidEntity;
import io.mosip.kernel.vidgenerator.exception.VidGeneratorServiceException;
import io.mosip.kernel.vidgenerator.repository.VidAssignedRepository;
import io.mosip.kernel.vidgenerator.repository.VidRepository;
import io.mosip.kernel.vidgenerator.service.VidService;
import io.mosip.kernel.vidgenerator.utils.ExceptionUtils;
import io.mosip.kernel.vidgenerator.utils.VIDMetaDataUtil;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;

@Service
public class VidServiceImpl implements VidService {

	private static final Logger LOGGER = LoggerFactory.getLogger(VidServiceImpl.class);

	@Value("${mosip.kernel.vid.time-to-release-after-expiry}")
	private long timeToRelaseAfterExpiry;

	@Value("${mosip.kernel.vid.vids-to-generate:500000}")
	private long vidsToGenerate;

	@Autowired
	private VidRepository vidRepository;

	@Autowired
	private VidAssignedRepository vidAssignedRepository;

	@Autowired
	private VIDMetaDataUtil metaDataUtil;

	@Autowired
	private VertxAuthenticationProvider authHandler;

	@Autowired
	private EntityManagerFactory entityManagerFactory;

	// Bloom filter — initialized ONCE at startup, never rebuilt per-call
	private volatile BloomFilter<CharSequence> vidBloomFilter;
	private final AtomicBoolean bloomFilterReady = new AtomicBoolean(false);
	private final CountDownLatch bloomFilterLatch = new CountDownLatch(1);

	@PostConstruct
	private void init() {
		// Run in background so pod starts fast and health probes pass immediately.
		Thread bloomInitThread = new Thread(this::initializeBloomFilter, "vid-bloom-filter-init");
		bloomInitThread.setDaemon(true);
		bloomInitThread.start();
	}

	/**
	 * Called ONCE from the background init thread. Loads all VIDs from both
	 * the active pool (vid table) and assigned pool (vid_assigned table) into the
	 * Bloom filter. Never called again — saveVID() updates the filter incrementally.
	 */
	private void initializeBloomFilter() {
		try {
			long vidCount = getExistingVidCountFromDB();
			long assignedCount = getExistingAssignedVidCountFromDB();
			long totalExisting = vidCount + assignedCount;
			long capacity = Math.max(totalExisting + vidsToGenerate, 100_000L);

			LOGGER.info("Initializing VID Bloom filter: capacity={}, vid={}, vid_assigned={}",
					capacity, vidCount, assignedCount);

			BloomFilter<CharSequence> filter = BloomFilter.create(
					Funnels.stringFunnel(StandardCharsets.UTF_8),
					capacity,
					0.001);

			EntityManager em = entityManagerFactory.createEntityManager();
			try {
				loadTableIntoFilter(em, "SELECT v.vid FROM VidEntity v", filter, "vid");
				loadTableIntoFilter(em, "SELECT v.vid FROM VidAssignedEntity v", filter, "vid_assigned");
			} finally {
				em.close();
			}

			// Publish atomically — no partial state visible to saveVID()
			vidBloomFilter = filter;
			bloomFilterReady.set(true);
			bloomFilterLatch.countDown();
			LOGGER.info("VID Bloom filter ready. Total entries loaded: {}", totalExisting);

		} catch (Exception e) {
			LOGGER.error("VID Bloom filter initialization failed", e);
			// Release latch so saveVID() doesn't hang; it will fall back to DB checks.
			bloomFilterLatch.countDown();
		}
	}

	private void loadTableIntoFilter(EntityManager em, String jpql,
			BloomFilter<CharSequence> filter, String tableName) {
		int batchSize = 10_000;
		int offset = 0;
		while (true) {
			List<String> vids = em.createQuery(jpql, String.class)
					.setFirstResult(offset)
					.setMaxResults(batchSize)
					.getResultList();
			if (vids.isEmpty()) break;
			vids.forEach(filter::put);
			offset += vids.size();
			LOGGER.info("VID Bloom filter: loaded {} entries from {}", offset, tableName);
		}
	}

	private long getExistingVidCountFromDB() {
		EntityManager em = entityManagerFactory.createEntityManager();
		try {
			return em.createQuery("SELECT COUNT(v) FROM VidEntity v", Long.class).getSingleResult();
		} finally {
			em.close();
		}
	}

	private long getExistingAssignedVidCountFromDB() {
		EntityManager em = entityManagerFactory.createEntityManager();
		try {
			return em.createQuery("SELECT COUNT(v) FROM VidAssignedEntity v", Long.class).getSingleResult();
		} finally {
			em.close();
		}
	}

	@Override
	@Transactional
	public VidFetchResponseDto fetchVid(LocalDateTime vidExpiry, RoutingContext routingContext) {
		VidFetchResponseDto vidFetchResponseDto = new VidFetchResponseDto();
		VidEntity vidEntity = null;
		try {
			vidEntity = vidRepository.findFirstByStatus(VidLifecycleStatus.AVAILABLE);
		} catch (DataAccessException exception) {
			LOGGER.error(ExceptionUtils.parseException(exception));
			throw new VidGeneratorServiceException(VIDGeneratorErrorCode.INTERNAL_SERVER_ERROR.getErrorCode(),
					exception.getMessage(), exception.getCause());
		} catch (Exception exception) {
			LOGGER.error(ExceptionUtils.parseException(exception));
			throw new VidGeneratorServiceException(VIDGeneratorErrorCode.INTERNAL_SERVER_ERROR.getErrorCode(),
					exception.getMessage(), exception.getCause());
		}
		if (vidEntity != null) {
			if (vidExpiry != null) {
				vidEntity.setVidExpiry(vidExpiry);
			}
			vidFetchResponseDto.setVid(vidEntity.getVid());
			try {
				vidRepository.updateVid(VidLifecycleStatus.ASSIGNED, authHandler.getContextUser(routingContext),
						DateUtils.getUTCCurrentDateTime(), vidEntity.getVid());
			} catch (DataAccessException exception) {
				LOGGER.error(ExceptionUtils.parseException(exception));
				throw new VidGeneratorServiceException(VIDGeneratorErrorCode.INTERNAL_SERVER_ERROR.getErrorCode(),
						exception.getMessage(), exception.getCause());
			} catch (Exception exception) {
				LOGGER.error(ExceptionUtils.parseException(exception));
				throw new VidGeneratorServiceException(VIDGeneratorErrorCode.INTERNAL_SERVER_ERROR.getErrorCode(),
						exception.getMessage(), exception.getCause());
			}
		} else {
			LOGGER.info("vid not available");
			throw new VidGeneratorServiceException(VIDGeneratorErrorCode.VID_NOT_AVAILABLE.getErrorCode(),
					VIDGeneratorErrorCode.VID_NOT_AVAILABLE.getErrorMessage());
		}
		return vidFetchResponseDto;
	}

	@Override
	public long fetchVidCount(String status) {
		long vidCount = 0;
		try {
			vidCount = vidRepository.countByStatusAndIsDeletedFalse(status);
		} catch (DataAccessException exception) {
			LOGGER.error(ExceptionUtils.parseException(exception));
		} catch (Exception exception) {
			LOGGER.error(ExceptionUtils.parseException(exception));
		}
		return vidCount;

	}

	@Override
	public void expireAndRelease() {
		try {
			expireEligibleVids();
			releaseEligibleVids();
		} catch (DataAccessException exception) {
			LOGGER.error(ExceptionUtils.parseException(exception));
		} catch (Exception exception) {
			LOGGER.error(ExceptionUtils.parseException(exception));
		}

	}

	private void expireEligibleVids() {
		List<VidAssignedEntity> vidAssignedEntities = vidAssignedRepository
			.findByStatusAndIsDeletedFalse(VidLifecycleStatus.ASSIGNED);
		vidAssignedEntities.forEach(this::expireIfEligible);
		vidAssignedRepository.saveAll(vidAssignedEntities);
	}

	private void releaseEligibleVids() {
		List<VidAssignedEntity> vidExpiredEntities = vidAssignedRepository
			.findByStatusAndIsDeletedFalse(VidLifecycleStatus.EXPIRED);
		List<VidAssignedEntity> releasableVidAssignedEntities = new ArrayList<VidAssignedEntity>();
		vidExpiredEntities.forEach(entity -> {
			if(isEligibleToRelease(entity)) {
				releasableVidAssignedEntities.add(entity);
			}
		});
		if(releasableVidAssignedEntities.size() > 0) {
			vidAssignedRepository.deleteAll(releasableVidAssignedEntities);
		}
	}

	private void expireIfEligible(VidAssignedEntity entity) {
		LocalDateTime currentTime = DateUtils.getUTCCurrentDateTime();
		LOGGER.debug("currenttime {} for checking entity with expiry time {}", currentTime, entity.getVidExpiry());
		if (entity.getVidExpiry() != null && (entity.getVidExpiry().isBefore(currentTime) || entity.getVidExpiry().isEqual(currentTime))
				&& entity.getStatus().equals(VidLifecycleStatus.ASSIGNED)) {
			metaDataUtil.setUpdateMetaData(entity);
			entity.setStatus(VidLifecycleStatus.EXPIRED);
		}
	}

	private boolean isEligibleToRelease(VidAssignedEntity entity) {
		LocalDateTime currentTime = DateUtils.getUTCCurrentDateTime();
		LocalDateTime releaseElegibleTime = entity.getVidExpiry().plusDays(timeToRelaseAfterExpiry);
		LOGGER.debug("currenttime {} for checking entity with release elegible time {}", currentTime, releaseElegibleTime);
		if ((releaseElegibleTime.isBefore(currentTime) || releaseElegibleTime.isEqual(currentTime))
				&& entity.getStatus().equals(VidLifecycleStatus.EXPIRED)) {
			return true;
		}
		return false;
	}

	@Override
	public boolean saveVID(VidEntity vid) {
		// Wait for Bloom filter only if not yet ready (happens only on the very first
		// call during startup, if pool population fires before init completes).
		if (!bloomFilterReady.get()) {
			try {
				boolean completed = bloomFilterLatch.await(120, TimeUnit.SECONDS);
				if (!completed) {
					LOGGER.warn("VID Bloom filter init timed out after 120s; falling back to DB checks");
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				LOGGER.error("Interrupted while waiting for VID Bloom filter", e);
			}
		}

		if (bloomFilterReady.get()) {
			// Fast path: Bloom filter says it definitely doesn't exist → skip DB checks.
			// A false positive (~0.1%) means we skip a valid VID — safe, generates another.
			if (vidBloomFilter.mightContain(vid.getVid())) {
				return false;
			}
			try {
				this.vidRepository.saveAndFlush(vid);
				vidBloomFilter.put(vid.getVid()); // update filter after confirmed insert
				return true;
			} catch (DataAccessException exception) {
				LOGGER.error(ExceptionUtils.parseException(exception));
				return false;
			} catch (Exception exception) {
				LOGGER.error(ExceptionUtils.parseException(exception));
				return false;
			}
		}

		// Fallback: Bloom filter unavailable — use original DB existence checks
		if (!(this.vidRepository.existsById(vid.getVid()) ||
				this.vidAssignedRepository.existsById(vid.getVid()))) {
			try {
				this.vidRepository.saveAndFlush(vid);
			} catch (DataAccessException exception) {
				LOGGER.error(ExceptionUtils.parseException(exception));
				return false;
			} catch (Exception exception) {
				LOGGER.error(ExceptionUtils.parseException(exception));
				return false;
			}
			return true;
		}
		return false;
	}

	@Transactional(transactionManager = "transactionManager")
	@Override
	public void isolateAssignedVids() {
		List<VidEntity> vidEntities = vidRepository.findByStatusAndIsDeletedFalse(VidLifecycleStatus.ASSIGNED);
		LOGGER.info("isolateAssignedVids called for entity count {} ", vidEntities.size());
		List<VidAssignedEntity> vidEntitiesAssined = convertVidEntitiesToVidAssignedEntity(vidEntities);
		vidAssignedRepository.saveAll(vidEntitiesAssined);
	    vidRepository.deleteAll(vidEntities);
	}

	private List<VidAssignedEntity> convertVidEntitiesToVidAssignedEntity(List<VidEntity> vidEntities) {
		return vidEntities.stream()
				.map(VidAssignedEntity::new)
				.collect(Collectors.toList());
	}
}