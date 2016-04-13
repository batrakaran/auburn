package com.adobe.auburn.migration.scheduler;


        import org.apache.felix.scr.annotations.*;
        import org.apache.sling.api.SlingConstants;
        import org.apache.sling.api.resource.LoginException;
        import org.apache.sling.api.resource.ResourceResolver;
        import org.apache.sling.api.resource.ResourceResolverFactory;
        import org.apache.sling.discovery.TopologyEvent;
        import org.apache.sling.discovery.TopologyEventListener;
        import org.apache.sling.event.jobs.JobManager;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import java.util.HashMap;
        import java.util.Map;

@Component(
        label = "ACS AEM Samples - Cluster Aware Scheduled Service",
        description = "Sample scheduled service using CQ 5.6/AEM6 TopologyEventListener method",
        immediate = true // Load immediately
)

@Properties({
        @Property(
                label = "Cron expression defining when this Scheduled Service will run",
                description = "[every minute = 0 * * * * ?] Visit www.cronmaker.com to generate cron expressions.",
                name = "scheduler.expression",
                value = "0 1 0 ? * *"
        ),
        @Property(
                label = "Allow concurrent executions",
                description = "Allow concurrent executions of this Scheduled Service. This is almost always false.",
                name = "scheduler.concurrent",
                propertyPrivate = true,
                boolValue = false
        )
})

@Service
public class AssetMigrationScheduler implements Runnable, TopologyEventListener {
    private final Logger log = LoggerFactory.getLogger(AssetMigrationScheduler.class);

    private static final String JOB_NAME = "com/adobe/auburn/asset-migration-job";

    @Reference
    private ResourceResolverFactory resourceResolverFactory;

    @Reference
    private JobManager jobManager;

    private boolean isLeader = false;

    @Override
    public void run() {
        // Scheduled services that do not have to be cluster aware do not need
        // to implement this check OR extend TopologyEventListener
        if (!isLeader) {
            return;
        }

        // Scheduled service logic, only run on the Master
        ResourceResolver adminResourceResolver = null;
        try {
            // Be careful not to leak the adminResourceResolver
            adminResourceResolver = resourceResolverFactory.getAdministrativeResourceResolver(null);

            // execute your scheduled service logic here ...

            log.debug(" CreateBrand handleEvent() {}");
            final Map<String, Object> payload = new HashMap<>();
            payload.put(SlingConstants.PROPERTY_PATH, "");
            payload.put("assetName", "payloadTitle");
            payload.put("assetType", "mimeType");
            payload.put("inputStream", "blob inputStream");

            // There must be a JobConsumer registered for this Topic
            jobManager.addJob(JOB_NAME, payload);


        } catch (LoginException e) {
            log.error("Error obtaining the admin resource resolver.", e);
        } finally {
            // ALWAYS close resolvers you open
            if (adminResourceResolver != null) {
                adminResourceResolver.close();
            }
        }
    }

    /** Topology Aware Methods **/

    @Override
    public void handleTopologyEvent(final TopologyEvent event) {
        if (event.getType() == TopologyEvent.Type.TOPOLOGY_CHANGED
                || event.getType() == TopologyEvent.Type.TOPOLOGY_INIT) {
            this.isLeader = event.getNewView().getLocalInstance().isLeader();
        }
    }
}