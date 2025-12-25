/*
 * Copyright (c) 2020, NanoKV.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.microraft.nanokv;

import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.microraft.nanokv.config.NanoKVConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static java.nio.file.Files.readAllLines;

public class RunNanoKV {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunNanoKV.class);

    public static void main(String[] args) {
        String configFileName = getConfigFileName(args);
        LOGGER.info("Reading config from " + configFileName);

        NanoKV server;
        NanoKVConfig config = readConfigFile(configFileName);
        if (config.getRaftGroupConfig().getJoinTo() == null) {
            server = NanoKV.bootstrap(config);
        } else {
            server = NanoKV.join(config, true /* votingMember */);
        }

        server.awaitTermination();
    }

    private static String getConfigFileName(String[] args) {
        String prop = System.getProperty("nanokv.config");
        if (args.length > 0) {
            return args[0];
        } else if (prop != null) {
            return prop;
        } else {
            LOGGER.error("Config file location must be provided either via program argument or system parameter: "
                    + "\"nanokv.config\"! If both are present, program argument is used.");
            System.exit(-1);
            return null;
        }
    }

    private static NanoKVConfig readConfigFile(String configFileName) {
        try {
            String config = String.join("\n", readAllLines(Paths.get(configFileName), StandardCharsets.UTF_8));
            return NanoKVConfig.from(ConfigFactory.parseString(config));
        } catch (IOException e) {
            LOGGER.error("Cannot read config file: " + configFileName, e);
            System.exit(-1);
            return null;
        }
    }

}
