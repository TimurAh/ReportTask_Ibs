package ATIBS.nifi.examples.reporting;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;
import java.io.File;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"TAkhmetov","IBS", "bulletin", "metrics"})
@CapabilityDescription("Report bulletin metrics")
public class BulletinReportingTask extends AbstractReportingTask {


    public static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name("Group Id")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("Log File Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected List<PropertyDescriptor> properties;
    private String groupId;
    private String fileName;
    @Override
    protected void init(final ReportingInitializationContext config){
        properties = new ArrayList<>();
        properties.add(GROUP_ID);
        properties.add(FILE_NAME);
    }
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {

        groupId = context.getProperty(GROUP_ID).getValue();


        fileName = context.getProperty(FILE_NAME).getValue();

    }

    protected AtomicLong lastQuery = new AtomicLong(0);

    protected BulletinQuery getBulletinQuery(final ReportingContext context){
        final ComponentLog log = getLogger();
        BulletinQuery.Builder queryBuilder = new BulletinQuery.Builder().after(lastQuery.get());

        if(null != groupId && !groupId.isEmpty() && !groupId.equals("*") ){

            queryBuilder.groupIdMatches(groupId);
        }

        return queryBuilder.build();
    }

    protected List<Bulletin> getBulletins(final ReportingContext context) {
        BulletinRepository repo = context.getBulletinRepository();
        BulletinQuery query = getBulletinQuery(context);
        List<Bulletin> bulletins = repo.findBulletins(query);

        if(!bulletins.isEmpty()){
            lastQuery.lazySet(bulletins.get(0).getId());
        }

        return bulletins;
    }
    @Override
    public void onTrigger(final ReportingContext context) {
        final ComponentLog log = getLogger();

        List<Bulletin> bulletins = getBulletins(context);

        String nifiRootDir = System.getProperty("user.dir");
        String filePath = nifiRootDir + File.separator + fileName + ".txt";


        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filePath), true))) {
                       for (Bulletin bulletin : bulletins) {
                String message = bulletin.getMessage();
                writer.write("Время ошибки:"+bulletin.getTimestamp()+".  " +message+"\n getGroupId: " + bulletin.getGroupId()+"\n getGroupName: " + bulletin.getGroupName()+"\n getGroupPath: " + bulletin.getGroupPath());
                writer.newLine();
            }
        } catch (IOException e) {
            log.error("Error writing to file: " + filePath, e);
        }

    }

}