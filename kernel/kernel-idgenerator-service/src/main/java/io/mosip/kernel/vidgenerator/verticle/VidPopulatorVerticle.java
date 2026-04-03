package io.mosip.kernel.vidgenerator.verticle;

import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import io.mosip.kernel.core.idgenerator.spi.VidGenerator;
import io.mosip.kernel.vidgenerator.constant.EventType;
import io.mosip.kernel.vidgenerator.constant.VidLifecycleStatus;
import io.mosip.kernel.vidgenerator.entity.VidEntity;
import io.mosip.kernel.vidgenerator.generator.VidWriter;
import io.mosip.kernel.vidgenerator.utils.VIDMetaDataUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class VidPopulatorVerticle extends AbstractVerticle {

	private static final Logger LOGGER = LoggerFactory.getLogger(VidPopulatorVerticle.class);

	private long vidToGenerate;

	private Environment environment;

	private VidWriter vidWriter;

	private VIDMetaDataUtil metaDataUtil;

	private VidGenerator<String> vidGenerator;

	@SuppressWarnings("unchecked")
	public VidPopulatorVerticle(final ApplicationContext context) {
		this.environment = context.getBean(Environment.class);
		this.vidToGenerate = Objects.requireNonNullElse(environment.getProperty("mosip.kernel.vid.vids-to-generate", Long.class), 0L);
		this.vidWriter = context.getBean("vidWriter", VidWriter.class);
		this.metaDataUtil = context.getBean(VIDMetaDataUtil.class);
		this.vidGenerator = context.getBean(VidGenerator.class);
	}

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		vertx.eventBus().consumer(EventType.GENERATEPOOL, handler -> {
			long noOfFreeVids = Long.parseLong(handler.body().toString());
			long noOfVidsToGenerate = vidToGenerate - noOfFreeVids;
			LOGGER.info("Persisting {} vids in pool", noOfVidsToGenerate);

			// Run on a worker thread — never block the Vert.x event loop.
			// Blocking the event loop delays health-check responses and can trigger
			// Kubernetes liveness probe failures → pod restart.
			vertx.executeBlocking(future -> {
				final int batchSize = 500;
				long count = 0;
				while (count < noOfVidsToGenerate) {
					List<VidEntity> batch = new ArrayList<>(batchSize);
					for (int i = 0; i < batchSize; i++) {
						String vid = vidGenerator.generateId();
						VidEntity entity = new VidEntity();
						entity.setVid(vid);
						entity.setStatus(VidLifecycleStatus.AVAILABLE);
						metaDataUtil.setCreateMetaData(entity);
						batch.add(entity);
					}
					count += vidWriter.persistBatch(batch);
				}
				LOGGER.info("No of vids persisted are {}", count);
				future.complete(count);
			}, result -> {
				if (result.succeeded()) {
					handler.reply("pool population successfull");
				} else {
					LOGGER.error("VID pool population failed", result.cause());
					handler.fail(500, result.cause() != null ? result.cause().getMessage() : "VID generation failed");
				}
			});
		});
	}
}
