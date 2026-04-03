package io.mosip.kernel.uingenerator.generator;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.PersistenceException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import io.mosip.kernel.core.idgenerator.spi.UinGenerator;
import io.mosip.kernel.core.util.ChecksumUtils;
import io.mosip.kernel.uingenerator.constant.UinGeneratorConstant;
import io.mosip.kernel.uingenerator.entity.UinEntity;
import io.mosip.kernel.uingenerator.service.UinService;
import io.mosip.kernel.uingenerator.util.UINMetaDataUtil;
import io.mosip.kernel.uingenerator.util.UinFilterUtil;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * This class generates a list of uins
 *
 * @author Dharmesh Khandelwal
 * @since 1.0.0
 */
@Component
public class UinGeneratorImpl implements UinGenerator {

	@Autowired
	private UinFilterUtil uinFilterUtils;

	@Autowired
	private UINMetaDataUtil metaDataUtil;

	@Autowired
	private UinService uinService;

	@Autowired
	private UinWriter uinWriter;

	@Autowired
	private EntityManagerFactory entityManagerFactory;

	private static final Logger LOGGER = LoggerFactory.getLogger(UinGeneratorImpl.class);

	private final long uinsCount;
	private final int uinLength;
	private final String uinDefaultStatus;

	private SecureRandom random;

	@Value("${mosip.idgen.uin.secure-random-reinit-frequency:45}")
	private int reInitSecureRandomFrequency;

	// Bloom filter state — initialized ONCE at startup, never rebuilt
	private volatile BloomFilter<CharSequence> uinBloomFilter;
	private final AtomicBoolean bloomFilterReady = new AtomicBoolean(false);
	private final CountDownLatch bloomFilterLatch = new CountDownLatch(1);

	public UinGeneratorImpl(@Value("${mosip.kernel.uin.uins-to-generate}") long uinsCount,
			@Value("${mosip.kernel.uin.length}") int uinLength) {
		this.uinsCount = uinsCount;
		this.uinLength = uinLength;
		this.uinDefaultStatus = UinGeneratorConstant.UNUSED;
	}

	@PostConstruct
	private void init() {
		// SecureRandom periodic re-init scheduler
		ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.setPoolSize(1);
		taskScheduler.initialize();
		taskScheduler.scheduleAtFixedRate(this::initializeSecureRandom,
				TimeUnit.MINUTES.toMillis(reInitSecureRandomFrequency));

		// Initialize Bloom filter in background so pod startup is non-blocking.
		// Health probes pass immediately; generateId() will wait only if the first
		// scheduler tick fires before initialization completes (rare in practice).
		Thread bloomInitThread = new Thread(this::initializeBloomFilter, "bloom-filter-init");
		bloomInitThread.setDaemon(true);
		bloomInitThread.start();
	}

	private void initializeSecureRandom() {
		random = new SecureRandom();
	}

	/**
	 * Called ONCE from the background init thread. Loads all existing UINs from DB
	 * into the Bloom filter. Never called again — generateId() updates the filter
	 * incrementally via bloomFilter.put() after each insert.
	 */
	private void initializeBloomFilter() {
		try {
			long existingCount = getExistingUinCountFromDB();
			long capacity = Math.max(existingCount + uinsCount, 100_000L);

			LOGGER.info("Initializing Bloom filter with capacity={}, existing UINs in DB={}", capacity, existingCount);

			BloomFilter<CharSequence> filter = BloomFilter.create(
					Funnels.stringFunnel(StandardCharsets.UTF_8),
					capacity,
					0.001);

			EntityManager em = entityManagerFactory.createEntityManager();
			try {
				int batchSize = 10_000;
				int offset = 0;
				while (true) {
					List<String> uins = em
							.createQuery("SELECT u.uin FROM UinEntity u", String.class)
							.setFirstResult(offset)
							.setMaxResults(batchSize)
							.getResultList();
					if (uins.isEmpty()) break;
					uins.forEach(filter::put);
					offset += uins.size();
					LOGGER.info("Bloom filter loaded {} / {} UINs", offset, existingCount);
				}
			} finally {
				em.close();
			}

			// Publish atomically — generateId() either sees null (waits) or the fully
			// loaded filter. No partial state is ever visible.
			uinBloomFilter = filter;
			bloomFilterReady.set(true);
			bloomFilterLatch.countDown();
			LOGGER.info("Bloom filter ready. Total UINs loaded: {}", existingCount);

		} catch (Exception e) {
			LOGGER.error("Bloom filter initialization failed", e);
			// Release latch so generateId() doesn't hang; it will fall back to DB check.
			bloomFilterLatch.countDown();
		}
	}

	private long getExistingUinCountFromDB() {
		EntityManager em = entityManagerFactory.createEntityManager();
		try {
			return em.createQuery("SELECT COUNT(u) FROM UinEntity u", Long.class).getSingleResult();
		} finally {
			em.close();
		}
	}

	@Override
	public void generateId(long noOfUINToGenerate) {
		if (noOfUINToGenerate <= 0) return;

		// Wait for Bloom filter to be ready (only blocks on the very first call if
		// pod startup init hasn't finished yet — typically a sub-second wait).
		if (!bloomFilterReady.get()) {
			LOGGER.info("Waiting for Bloom filter initialization to complete...");
			try {
				boolean completed = bloomFilterLatch.await(120, TimeUnit.SECONDS);
				if (!completed) {
					LOGGER.warn("UIN Bloom filter init timed out after 120s; falling back to legacy path");
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				LOGGER.error("Interrupted while waiting for Bloom filter", e);
				return;
			}
		}

		// If init failed, bloomFilterReady is still false — fall back to legacy path.
		if (!bloomFilterReady.get()) {
			LOGGER.warn("Bloom filter unavailable; falling back to DB-based duplicate check");
			generateIdLegacy(noOfUINToGenerate);
			return;
		}

		LOGGER.info("Starting UIN generation: requested={}", noOfUINToGenerate);
		long startTime = System.nanoTime();

		int generatedIdLength = uinLength - 1;
		long upperBound = Long.parseLong(StringUtils.repeat(UinGeneratorConstant.NINE, generatedIdLength));
		long lowerBound = Long.parseLong(StringUtils.repeat(UinGeneratorConstant.ZERO, generatedIdLength));

		int batchSize = 5_000;
		EntityManager em = entityManagerFactory.createEntityManager();
		try {
			long count = 0;
			List<UinEntity> batch = new ArrayList<>(batchSize);

			while (count < noOfUINToGenerate) {
				String uin = generateSingleId(generatedIdLength, lowerBound, upperBound);

				// Bloom filter check: cheap probabilistic gate (0.1% false positive rate).
				// A false positive means we skip a valid UIN — acceptable and safe.
				// A false negative is impossible by design, so no real duplicate reaches DB.
				if (!uinBloomFilter.mightContain(uin) && uinFilterUtils.isValidId(uin)) {
					uinBloomFilter.put(uin); // update immediately to prevent intra-batch duplicates

					UinEntity entity = new UinEntity(uin, uinDefaultStatus);
					metaDataUtil.setCreateMetaData(entity);
					batch.add(entity);

					if (batch.size() >= batchSize || (count + batch.size()) >= noOfUINToGenerate) {
						int inserted = insertBatch(em, batch);
						count += inserted;
						batch.clear();
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("UIN generation failed", e);
		} finally {
			em.close();
		}

		long durationMs = (System.nanoTime() - startTime) / 1_000_000;
		LOGGER.info("Generated {} UINs in {} ms (~{} s)", noOfUINToGenerate, durationMs, durationMs / 1000);
	}

	/**
	 * Legacy path used only when Bloom filter initialization fails.
	 * Identical to the original implementation — DB checked per UIN.
	 */
	private void generateIdLegacy(long noOfUINToGenerate) {
		int generatedIdLength = uinLength - 1;
		long uinCount = 0;
		long upperBound = Long.parseLong(StringUtils.repeat(UinGeneratorConstant.NINE, generatedIdLength));
		long lowerBound = Long.parseLong(StringUtils.repeat(UinGeneratorConstant.ZERO, generatedIdLength));
		uinWriter.setSession();
		while (uinCount < noOfUINToGenerate) {
			String generatedUIN = generateSingleId(generatedIdLength, lowerBound, upperBound);
			if (uinFilterUtils.isValidId(generatedUIN) && !uinService.uinExist(generatedUIN)) {
				UinEntity uinBean = new UinEntity(generatedUIN, uinDefaultStatus);
				metaDataUtil.setCreateMetaData(uinBean);
				uinWriter.persistUin(uinBean);
				uinCount++;
			}
		}
		uinWriter.closeSession();
		LOGGER.info("Generated {} uins (legacy path)", noOfUINToGenerate);
	}

	private String generateSingleId(int generatedIdLength, long lowerBound, long upperBound) {
		if (random == null) {
			initializeSecureRandom();
		}
		long range = upperBound - lowerBound + 1;
		long randomNumber = (Math.abs(random.nextLong()) % range) + lowerBound;
		String generatedID = String.format("%0" + generatedIdLength + "d", randomNumber);
		String verhoeffDigit = ChecksumUtils.generateChecksumDigit(generatedID);
		return generatedID + verhoeffDigit;
	}

	private int insertBatch(EntityManager em, List<UinEntity> batch) {
		if (batch == null || batch.isEmpty()) return 0;

		// Filter out any UINs that somehow already exist (Bloom false positives can't
		// cause this, but a concurrent process might have inserted them).
		List<String> uinStrings = batch.stream().map(UinEntity::getUin).toList();
		Set<String> existingSet = new HashSet<>(
				em.createQuery("SELECT u.uin FROM UinEntity u WHERE u.uin IN :uins", String.class)
						.setParameter("uins", uinStrings)
						.getResultList());

		List<UinEntity> toInsert = batch.stream()
				.filter(u -> !existingSet.contains(u.getUin()))
				.toList();

		if (toInsert.isEmpty()) return 0;

		EntityTransaction tx = em.getTransaction();
		try {
			tx.begin();
			toInsert.forEach(em::persist);
			em.flush();
			em.clear();
			tx.commit();
			return toInsert.size();
		} catch (PersistenceException e) {
			if (tx.isActive()) tx.rollback();
			LOGGER.warn("Batch insert failed, retrying individually: {}", e.getMessage());
			return insertIndividually(em, toInsert);
		}
	}

	private int insertIndividually(EntityManager em, List<UinEntity> entities) {
		int inserted = 0;
		for (UinEntity entity : entities) {
			EntityTransaction tx = em.getTransaction();
			try {
				long exists = em.createQuery("SELECT COUNT(u) FROM UinEntity u WHERE u.uin = :uin", Long.class)
						.setParameter("uin", entity.getUin())
						.getSingleResult();
				if (exists == 0) {
					tx.begin();
					em.persist(entity);
					em.flush();
					tx.commit();
					inserted++;
				}
			} catch (Exception e) {
				if (tx.isActive()) tx.rollback();
				LOGGER.warn("Retry insert failed for UIN: {}", entity.getUin());
			}
		}
		return inserted;
	}

}
