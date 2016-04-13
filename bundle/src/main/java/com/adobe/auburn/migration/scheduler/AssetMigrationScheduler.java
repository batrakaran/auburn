package com.adobe.auburn.migration.scheduler;


        import com.adobe.auburn.migration.util.ContentMigrationUtiils;
        import com.day.commons.datasource.poolservice.DataSourceNotFoundException;
        import com.day.commons.datasource.poolservice.DataSourcePool;
        import org.apache.felix.scr.annotations.*;
        import org.apache.sling.api.SlingConstants;
        import org.apache.sling.api.resource.*;
        import org.apache.sling.discovery.TopologyEvent;
        import org.apache.sling.discovery.TopologyEventListener;
        import org.apache.sling.event.jobs.JobManager;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import java.nio.charset.StandardCharsets;
        import java.sql.Connection;
        import java.sql.ResultSet;
        import java.sql.SQLException;
        import java.sql.Statement;
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
                value = "0 0/1 * 1/1 * ? *"
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

    @Reference
    DataSourcePool dspService;

    private boolean isLeader = false;

    @Override
    public void run() {
        // Scheduled services that do not have to be cluster aware do not need
        // to implement this check OR extend TopologyEventListener
        if (!isLeader) {
            return;
        }

        log.info(" AssetMigrationScheduler  Start {}");

        // Scheduled service logic, only run on the Master
        ResourceResolver resourceResolver = null;
        try {
            // Be careful not to leak the adminResourceResolver
            resourceResolver = resourceResolverFactory.getAdministrativeResourceResolver(null);

            // DB conn
            Connection connection = ContentMigrationUtiils.getConnection(dspService);
            String assetCountPath = "/apps/auburn/components/jdbc/assets";
            Resource assetCountRes = resourceResolver.getResource(assetCountPath);
            ModifiableValueMap modifiableValueMap =  assetCountRes.adaptTo(ModifiableValueMap.class);
           // String count = (String) modifiableValueMap.get("startcount");
            //Resource parentRes = ResourceUtil.getOrCreateResource(resourceResolver, "/content/dam/auburn/migrated", null ,null, true);
            Resource parentRes = resourceResolver.getResource("/content/dam/auburn/migrated");

            String query = "SELECT * from assets";

            final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(query);

            int r=0;
            String title, type;
            //iterate the RS
            while(resultSet.next()){
                r=r+1;
                title  = resultSet.getString("title");
                type = resultSet.getString("type");
                byte[] content = resultSet.getBytes("content");

                final Map<String, Object> payload = new HashMap<>();
                payload.put("assetName", title);
                payload.put("assetType", type);
                String str = new String(content, StandardCharsets.UTF_8);
                payload.put("inputStream", str);
                payload.put("parentPath", parentRes.getPath());

                // There must be a JobConsumer registered for this Topic
                jobManager.addJob(JOB_NAME, payload);
                //TODO
                //execute jobs

                //end connection
                resultSet.close();
                connection.close();

            }

            //update the count value
            //int updatedValue = Integer.valueOf(count) + 100;
            //modifiableValueMap.put("startcount", Integer.toString(updatedValue));
            resourceResolver.commit();

            // execute your scheduled service logic here ...
            log.debug(" CreateBrand handleEvent() {}");

            } catch (DataSourceNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (LoginException e) {
            log.error("Error obtaining the admin resource resolver.", e);
            } catch (PersistenceException e) {
            e.printStackTrace();
        } finally {
            // ALWAYS close resolvers you open
            if (resourceResolver != null) {
                resourceResolver.close();
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