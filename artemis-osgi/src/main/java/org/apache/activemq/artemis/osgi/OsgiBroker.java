/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.osgi;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.jboss.logging.Logger;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class OsgiBroker {

   private static final Logger LOGGER = Logger.getLogger(OsgiBroker.class);

   private final String name;

   private final String configurationUrl;

   private final String brokerInstance;

   private boolean started;

   private final ActiveMQSecurityManager securityManager;

   private Map<String, ActiveMQComponent> components;

   private Map<String, ServiceRegistration<?>> registrations;

   public OsgiBroker(String name, String brokerInstance, String configuration, ActiveMQSecurityManager security) {
      this.name = name;
      this.brokerInstance = brokerInstance;
      this.securityManager = security;
      this.configurationUrl = configuration;
   }

   public synchronized void start() throws Exception {
      if (started) {
         return;
      }

      //todo if we start to pullout more configs from the main config then we should pull out the configuration objects from factories if available
      FileConfiguration configuration = new FileConfiguration();
      if (brokerInstance != null) {
         configuration.setBrokerInstance(new File(brokerInstance));
      }
      FileJMSConfiguration jmsConfiguration = new FileJMSConfiguration();

      FileDeploymentManager fileDeploymentManager = new FileDeploymentManager(configurationUrl);
      fileDeploymentManager.addDeployable(configuration).addDeployable(jmsConfiguration);
      fileDeploymentManager.readConfiguration();

      components = fileDeploymentManager.buildService(securityManager, ManagementFactory.getPlatformMBeanServer());

      ArrayList<ActiveMQComponent> componentsByStartOrder = getComponentsByStartOrder(components);
      LOGGER.info("ActiveMQ Artemis broker " + name + " starting");
      for (ActiveMQComponent component : componentsByStartOrder) {
         component.start();
      }
      started = true;

   }

   public void stop() throws Exception {
      if (!started) {
         return;
      }
      ActiveMQComponent[] mqComponents = new ActiveMQComponent[components.size()];
      components.values().toArray(mqComponents);
      for (int i = mqComponents.length - 1; i >= 0; i--) {
         mqComponents[i].stop();
      }
      started = false;
   }

   public boolean isStarted() {
      return started;
   }

   public Map<String, ActiveMQComponent> getComponents() {
      return components;
   }

   /*
   * this makes sure the components are started in the correct order. Its simple at the mo as e only have core and jms but
   * will need impproving if we get more.
   * */
   public ArrayList<ActiveMQComponent> getComponentsByStartOrder(Map<String, ActiveMQComponent> components) {
      ArrayList<ActiveMQComponent> activeMQComponents = new ArrayList<ActiveMQComponent>();
      ActiveMQComponent jmsComponent = components.get("jms");
      if (jmsComponent != null) {
         activeMQComponents.add(jmsComponent);
      }
      activeMQComponents.add(components.get("core"));
      return activeMQComponents;
   }

   public void register(BundleContext context, Dictionary<String, ?> properties) {
      registrations = new HashMap<>();
      for (Map.Entry<String, ActiveMQComponent> component : getComponents().entrySet()) {
         String[] classes = getInterfaces(component.getValue());
         Hashtable<String, Object> props = new Hashtable<>();
         for (Enumeration<String> keyEnum = properties.keys(); keyEnum.hasMoreElements();) {
            String key = keyEnum.nextElement();
            Object val = properties.get(key);
            props.put(key, val);
         }
         ServiceRegistration<?> registration = context.registerService(classes, component.getValue(), props);
         registrations.put(component.getKey(), registration);
      }
   }

   private String[] getInterfaces(ActiveMQComponent value) {
      Set<String> interfaces = new HashSet<>();
      getInterfaces(value.getClass(), interfaces);
      return interfaces.toArray(new String[interfaces.size()]);
   }

   private void getInterfaces(Class<?> clazz, Set<String> interfaces) {
      for (Class itf : clazz.getInterfaces()) {
         if (interfaces.add(itf.getName())) {
            getInterfaces(itf, interfaces);
         }
      }
      if (clazz.getSuperclass() != null) {
         getInterfaces(clazz.getSuperclass(), interfaces);
      }
   }

   public void unregister() {
      if (registrations != null) {
         for (ServiceRegistration<?> reg : registrations.values()) {
            reg.unregister();
         }
      }
   }
}
