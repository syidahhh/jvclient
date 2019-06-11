/**
 * SamplesController
 *
 * Executes a basic Sentilo Java Client Platform example which connects to the server and publish some data to a sample sensor.
 * In this case we're getting info from the system with the runtime properties object
 *
 * @author openTrends
 */
@Controller
public class SamplesController {

  private final Logger logger = LoggerFactory.getLogger(SamplesController.class);

  private static final String VIEW_SAMPLES_RESPONSE = "samples";

  private static final int SLEEP_TIME = 1;

  @Autowired
  private PlatformTemplate platformTemplate;

  @Resource
  private Properties samplesProperties;

  @RequestMapping(value = {"/", "/home"})
  public String runSamples(final Model model) {

    // All this data must be created in the Catalog Application before starting this
    // sample execution. At least the identity key and the provider id must be
    // declared in the system
    String restClientIdentityKey = samplesProperties.getProperty("rest.client.identityKey");
    String providerId = samplesProperties.getProperty("rest.client.provider");

    // For this example we have created a generic component with a status sensor that accepts text
    // type observations, only for test purpose
    String componentId = samplesProperties.getProperty("rest.client.component");
    String sensorId = samplesProperties.getProperty("rest.client.sensor");

    logger.info("Starting samples execution...");

    String observationsValue = null;
    String errorMessage = null;

    try {
      // Get some system data from runtime
      Runtime runtime = Runtime.getRuntime();
      NumberFormat format = NumberFormat.getInstance();
      StringBuilder sb = new StringBuilder();
      long maxMemory = runtime.maxMemory();
      long allocatedMemory = runtime.totalMemory();
      long freeMemory = runtime.freeMemory();

      sb.append("free memory: " + format.format(freeMemory / 1024) + "<br/>");
      sb.append("allocated memory: " + format.format(allocatedMemory / 1024) + "<br/>");
      sb.append("max memory: " + format.format(maxMemory / 1024) + "<br/>");
      sb.append("total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024) + "<br/>");

      // In this case, we're getting CPU status in text mode
      observationsValue = sb.toString();

      logger.info("Observations values: " + observationsValue);

      // Create the sample sensor, only if it doesn't exists in the catalog
      createSensorIfNotExists(restClientIdentityKey, providerId, componentId, sensorId);

      // Publish observations to the sample sensor
      sendObservations(restClientIdentityKey, providerId, componentId, sensorId, observationsValue);
    } catch (Exception e) {
      logger.error("Error publishing sensor observations: " + e.getMessage(), e);
      errorMessage = e.getMessage();
    }

    logger.info("Samples execution ended!");

    model.addAttribute("restClientIdentityKey", restClientIdentityKey);
    model.addAttribute("providerId", providerId);
    model.addAttribute("componentId", componentId);
    model.addAttribute("sensorId", sensorId);
    model.addAttribute("observations", observationsValue);

    ObjectMapper mapper = new ObjectMapper();

    try {
      if (errorMessage != null && errorMessage.length() > 0) {
        Object json = mapper.readValue(errorMessage, Object.class);
        model.addAttribute("errorMsg", mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
      } else {
        model.addAttribute("successMsg", "Observations sended successfully");
      }
    } catch (Exception e) {
      logger.error("Error parsing JSON: {}", e.getMessage(), e);
      errorMessage += (errorMessage.length() > 0) ? "<br/>" : "" + e.getMessage();
      model.addAttribute("errorMsg", errorMessage);
    }

    return VIEW_SAMPLES_RESPONSE;
  }

  /**
   * Retrieve catalog information about the sample provider. If the component and/or sensor doesn't
   * exists, it will create them
   *
   * @param identityToken Sample identity token
   * @param providerId Samples provider id
   * @param componentId Samples component id
   * @param sensorId Samples sensor id
   * @return {@link CatalogOutputMessage} object with provider's catalog data
   */
  private CatalogOutputMessage createSensorIfNotExists(String identityToken, String providerId, String componentId, String sensorId) {
    List<String> sensorsIdList = new ArrayList<String>();
    sensorsIdList.add(sensorId);

    // Create a CatalogInputMessage object for retrieve server data
    CatalogInputMessage getSensorsInputMsg = new CatalogInputMessage();
    getSensorsInputMsg.setProviderId(providerId);
    getSensorsInputMsg.setIdentityToken(identityToken);
    getSensorsInputMsg.setComponents(createComponentsList(componentId));
    getSensorsInputMsg.setSensors(createSensorsList(providerId, componentId, sensorsIdList));

    // Obtain the sensors list from provider within a CatalogOutputMessage response object type
    CatalogOutputMessage getSensorsOutputMsg = platformTemplate.getCatalogOps().getSensors(getSensorsInputMsg);

    // Search for the sensor in the list
    boolean existsSensor = false;
    if (getSensorsOutputMsg.getProviders() != null && !getSensorsOutputMsg.getProviders().isEmpty()) {
      for (AuthorizedProvider provider : getSensorsOutputMsg.getProviders()) {
        if (provider.getSensors() != null && !provider.getSensors().isEmpty()) {
          for (CatalogSensor sensor : provider.getSensors()) {
            logger.debug("Retrieved sensor: " + sensor.getComponent() + " - " + sensor.getSensor());
            existsSensor |= sensorId.equals(sensor.getSensor());
            if (existsSensor) {
              break;
            }
          }
        }
      }
    }

    // If the sensor doesn't exists in the retrieved list, we must create it before publishing the
    // observations
    if (!existsSensor) {
      // Create a CatalogInputMessage object for retrieve server data
      CatalogInputMessage registerSensorsInputMsg = new CatalogInputMessage(providerId);
      registerSensorsInputMsg.setIdentityToken(identityToken);
      registerSensorsInputMsg.setComponents(createComponentsList(componentId));
      registerSensorsInputMsg.setSensors(createSensorsList(providerId, componentId, sensorsIdList));

      // Register the new sensor in the server
      platformTemplate.getCatalogOps().registerSensors(registerSensorsInputMsg);
    }

    return getSensorsOutputMsg;
  }

  /**
   * Publish some observations from a sensor
   *
   * @param identityToken Samples Application identity token for manage the rest connections
   * @param providerId Samples provider id
   * @param componentId Samples component id
   * @param sensorId Samples sensor id
   * @param value Observations value, in our case, a String type
   */
  private void sendObservations(String identityToken, String providerId, String componentId, String sensorId, String value) {
    List<String> sensorsIdList = new ArrayList<String>();
    sensorsIdList.add(sensorId);
    createSensorsList(providerId, componentId, sensorsIdList);

    List<Observation> observations = new ArrayList<Observation>();
    Observation observation = new Observation(value, new Date());
    observations.add(observation);

    SensorObservations sensorObservations = new SensorObservations(sensorId);
    sensorObservations.setObservations(observations);

    DataInputMessage dataInputMessage = new DataInputMessage(providerId, sensorId);
    dataInputMessage.setIdentityToken(identityToken);
    dataInputMessage.setSensorObservations(sensorObservations);

    platformTemplate.getDataOps().sendObservations(dataInputMessage);
  }

  /**
   * Create a component list
   *
   * @param componentId Component identifier
   * @return A {@link CatalogComponent} list
   */
  private List<CatalogComponent> createComponentsList(String componentId) {
    List<CatalogComponent> catalogComponentList = new ArrayList<CatalogComponent>();
    CatalogComponent catalogComponent = new CatalogComponent();
    catalogComponent.setComponent(componentId);
    catalogComponent.setComponentType(samplesProperties.getProperty("rest.client.component.type"));
    catalogComponent.setLocation(samplesProperties.getProperty("rest.client.component.location"));
    catalogComponentList.add(catalogComponent);
    return catalogComponentList;
  }

  /**
   * Create a sensor list
   *
   * @param componentId The Sample Component Id
   * @param sensorsIdList A list with the sensor ids to create
   * @return A {@link CatalogSensor} list
   */
  private List<CatalogSensor> createSensorsList(String providerId, String componentId, List<String> sensorsIdList) {
    List<CatalogSensor> catalogSensorsList = new ArrayList<CatalogSensor>();
    for (String sensorId : sensorsIdList) {
      CatalogSensor catalogSensor = new CatalogSensor();
      catalogSensor.setComponent(componentId);
      catalogSensor.setSensor(sensorId);
      catalogSensor.setProvider(providerId);
      catalogSensor.setType(samplesProperties.getProperty("rest.client.sensor.type"));
      catalogSensor.setDataType(samplesProperties.getProperty("rest.client.sensor.dataType"));
      catalogSensor.setLocation(samplesProperties.getProperty("rest.client.sensor.location"));
      catalogSensorsList.add(catalogSensor);
    }
    return catalogSensorsList;
  }
}
