package com.adobe.auburn.migration.job;

import org.apache.commons.lang3.StringUtils;
import org.apache.felix.scr.annotations.*;
import org.apache.sling.api.SlingConstants;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


@Component(
        label = "Migrate Asset Job - Sling Job Consumer",
        description = "Migrate Asset Job  - Sling Job Consumer",
        // One of the few cases where immediate = true; this is so the Event Listener starts listening immediately
        immediate = true
)
@Properties({
        @Property(
                label = "Job Topics",
                value = {"com/adobe/auburn/asset-migration-job"},
                description = "[Required] Job Topics this job consumer will to respond to.",
                name = JobConsumer.PROPERTY_TOPICS,
                propertyPrivate = true
        )
})
@Service
public class MigrateAssetJob implements JobConsumer {

    private static final Logger log = LoggerFactory.getLogger(MigrateAssetJob.class);

    @Reference
    ResourceResolverFactory resourceResolverFactory;


    @Override
    public JobResult process(final Job job) {

        // This is the Job's process method where the work will be

        // Jobs status is persisted in the JCR under /var/eventing so the management
        // of Jobs is NOT a wholly "in-memory" operations.

        // If you have guaranteed VERY FAST processing, it may be better to tie into an event

        // For information on all the data tied to the Job object
        // > http://sling.apache.org/apidocs/sling7/org/apache/sling/event/jobs/Job.html
        Map<String, Object> authenticationInfo = new HashMap<>();
        authenticationInfo.put(ResourceResolverFactory.SUBSERVICE, null);

        ResourceResolver resourceResolver = null;
        try {
            resourceResolver = resourceResolverFactory.getServiceResourceResolver(authenticationInfo);
            log.debug(resourceResolver.getUserID());
            String path = (String) job.getProperty(SlingConstants.PROPERTY_PATH);
           // String pageName = (String) job.getProperty(Constants.PN_NAME);
           // String template = (String) job.getProperty(Constants.PN_TEMPLATE);
           // String pageTitle = (String) job.getProperty(Constants.PN_TITLE);


            if(resourceResolver != null && StringUtils.isNotBlank(path)){

                log.debug(" path in job {}", path);

                Resource parentResource = resourceResolver.getResource(path);

                //TODO... stuff goes here

            }


            } catch (LoginException e) {
            e.printStackTrace();
            } finally {
            if (resourceResolver != null && resourceResolver.isLive()) {
                resourceResolver.close();
            }
        }


        /**
         * Return the proper JobResult based on the work done...
         *
         * > OK : Processed successfully
         * > FAILED: Processed unsuccessfully and reschedule
         * > CANCEL: Processed unsuccessfully and do NOT reschedule
         * > ASYNC: Process through the JobConsumer.AsyncHandler interface
         */
        return JobResult.OK;
    }

}