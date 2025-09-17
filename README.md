# DataLakeHouseMCP Java: Model Context Protocol (MCP) Server

## Overview

DataLakeHouseMCP Java is a Spring Boot-based MCP server that exposes resources and tools for interacting with data infrastructure components such as Kafka, Flink, and Trino/Iceberg. The server is designed for discoverability and integration with AI-powered MCP clients like Copilot Chat (VSCode/IntelliJ), Claude Desktop, and MCP Inspector.

## Prerequisites

- **Lakehouse Setup**: You must have a local lakehouse environment running with Kafka, Flink, Trino, and Iceberg. For a quickstart, follow the instructions in the [flink-iceberg GitHub repository](https://github.com/zabi82/flink-iceberg):
  - Clone the repo and run the provided docker-compose setup.
  - This will start Kafka (with Schema Registry), Flink, Trino, and Iceberg with sample data and tables.
  - Example setup:
    ```bash
    git clone https://github.com/zabi82/flink-iceberg.git
    cd flink-iceberg
    docker compose up -d
    ```
  - Wait for all services to be healthy before starting the MCP server.
- **Java 21+** must be installed. Download from [Adoptium](https://adoptium.net/).
- **Maven** for dependency management and building the project.

## File Structure

- `src/main/java/com/mcp/datalakehouse/McpServerApplication.java`: MCP server entry point.
- `src/main/java/com/mcp/datalakehouse/tool/KafkaToolService.java`: Kafka-related MCP tools.
- `src/main/java/com/mcp/datalakehouse/tool/FlinkToolService.java`: Flink-related MCP tools.
- `src/main/java/com/mcp/datalakehouse/tool/TrinoToolService.java`: Trino/Iceberg-related MCP tools.
- `src/main/resources/application.properties`: Configuration properties for all tools.
- `pom.xml`: Maven dependencies.

## Features & Tool Capabilities

### Kafka Tools
- **List Kafka Topics**: Lists all Kafka topics available in the local cluster.
- **Peek Kafka Topic**: Retrieves the latest N messages from a specified Kafka topic. Supports:
  - Avro messages (using Schema Registry)
  - JSON string messages
  - Plain text messages
  - Automatic deserializer selection and fallback
  - Configurable number of messages (default: 10)

### Flink Tools
- **Cluster Overview**: Shows Flink cluster metrics: number of task managers, slots, jobs running/finished/cancelled/failed.
- **JobManager Metrics**: Returns JobManager metrics (heap memory, CPU load, JVM/process stats).
- **TaskManagers Metrics**: Returns TaskManagers metrics (heap memory, network IO, slot utilization).
- **List Flink Jobs**: Lists all Flink jobs running on the cluster (IDs, names, status).
- **Flink Job Details**: Returns details for one or more Flink jobs by job ID(s): status, vertices, configuration.
- **List TaskManagers**: Lists all Flink TaskManagers and their details.
- **Probe JobManager/TaskManager Metrics**: Query specific metrics by name.

### Trino & Iceberg Tools
- **List Iceberg Tables**: Lists all Iceberg tables in a specified Trino catalog.
- **List Trino Catalogs**: Lists all catalogs available in the Trino cluster.
- **List Trino Schemas**: Lists all schemas in one or more specified Trino catalogs (multi-catalog supported).
- **Get Iceberg Table Schema**: Returns the schema (columns/types) of an Iceberg table.
- **Execute Trino Query**: Executes a SQL query on Trino and returns results.
- **Iceberg Time Travel Query**: Executes a time travel query on Iceberg tables using Trino. Supports ISO 8601 timestamp or snapshot ID.
- **List Iceberg Snapshots**: Lists all snapshots for a given Iceberg table.

### MCP Discovery & Inspector
- All tools/resources are annotated for easy discovery by MCP clients.
- Use [MCP Inspector](https://github.com/zabi82/mcp-inspector) to visually inspect available tools, schemas, and try out tool invocations interactively.

## Installation

1. **Clone the repository**

```bash
git clone <your-repo-url>
cd DataLakeHouseMCPJava
```

2. **Build the project**

```bash
mvn clean install
```

## Running the MCP Server

This MCP server runs as a Spring Boot application. It can be launched via Maven or directly using the generated JAR file.

```bash
mvn spring-boot:run
# or
java -jar target/datalakehouse-mcp-java-0.1.0.jar
```

## Configuring MCP Clients

### Claude Desktop

1. Go to Settings > Integrations > Model Context Protocol (MCP).
2. Click "Add MCP Server" and set the executable path to your MCP server (e.g. `java`).
3. Set arguments to: `-jar /path/to/DataLakeHouseMCPJava/target/datalakehouse-mcp-java-0.1.0.jar`
4. Optionally, set the working directory to your project folder (e.g. `/path/to/DataLakeHouseMCPJava`).
5. Save and enable the integration.
6. Claude will launch the MCP server in stdio mode and auto-discover available MCP tools and resources.
7. Example `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "mcp-data-lakehouse-java": {
      "command": "java",
      "args": [
        "-jar",
        "/path/to/DataLakeHouseMCPJava/target/datalakehouse-mcp-java-0.1.0.jar"
      ]
    }
  }
}
```

### Copilot Chat in VSCode

1. Open Copilot Chat and go to MCP server configuration (usually in the extension settings or via command palette).
2. Add a new MCP server:
   - Executable: `java`
   - Arguments: `-jar /path/to/DataLakeHouseMCPJava/target/datalakehouse-mcp-java-0.1.0.jar`
   - Working directory: `/path/to/DataLakeHouseMCPJava`
3. Save the configuration.
4. Example `mcp.json`:
```json
{
  "servers": {
    "mcp-data-lakehouse-java-test": {
      "type": "stdio",
      "command": "java",
      "args": ["-jar", "/path/to/DataLakeHouseMCPJava/target/datalakehouse-mcp-java-0.1.0.jar"]
    }
  },
  "inputs": []
}
```

### Copilot Chat in IntelliJ

1. Open Copilot Chat and go to MCP server configuration (usually in plugin settings).
2. Add a new MCP server:
   - Executable: `java`
   - Arguments: `-jar /path/to/DataLakeHouseMCPJava/target/datalakehouse-mcp-java-0.1.0.jar`
   - Working directory: `/path/to/DataLakeHouseMCPJava`
3. Save the configuration.
4. Example `mcp.json`:
```json
{
  "servers": {
    "mcp-data-lakehouse-java": {
      "type": "stdio",
      "command": "java",
      "args": [
        "-jar",
        "/path/to/DataLakeHouseMCPJava/target/datalakehouse-mcp-java-0.1.0.jar"
      ]
    }
  }
}
```

## Example Prompts

You can use the following prompts in any MCP-enabled client:
- "List all Kafka topics."
- "Peek 10 messages from topic 'iceberg_users'."
- "Show Flink cluster metrics."
- "List Iceberg tables in Trino."
- "Run a query on Iceberg table."
- "Show Flink jobs."
- "Get schema for table ice_users."
- "Time travel query on Iceberg table."
- "List Trino catalogs."
- "Get details of Flink job 123."
- "List snapshots for iceberg table."
- "List all Flink TaskManagers and their details."
- "Probe JobManager metric 'Status.JVM.CPU.Load'."

## MCP Tool Discovery

All tools are annotated with descriptions. MCP clients and MCP Inspector will auto-discover available tools and their parameters, making it easy to interact programmatically or via chat.

## Extending

Add new tools/resources by creating service classes in `src/main/java/com/mcp/datalakehouse/tool/` and annotating with appropriate Spring AI MCP annotations. Follow the structure of existing tool services for best practices.

## Testing & Troubleshooting

### MCP Inspector Tool

You can use the [MCP Inspector](https://www.npmjs.com/package/@modelcontextprotocol/inspector) to test and troubleshoot the MCP server and its tools. This is especially useful for verifying tool interfaces, inspecting tool annotations, and simulating LLM interactions.

#### Usage

Run the following command in your project directory:

```
npx @modelcontextprotocol/inspector java -jar /path/to/DataLakeHouseMCPJava/target/datalakehouse-mcp-java-0.1.0.jar
```

This will start the MCP Inspector in stdio mode, allowing you to interactively test tool definitions and server responses. For more details, see the [MCP Inspector documentation](https://github.com/modelcontextprotocol/inspector).

- Ensure all dependencies are installed via Maven.
- Check MCP client configuration for correct executable path and arguments.
- Review logs for errors (e.g., missing modules, connection issues).
- Use MCP Inspector for interactive testing and debugging of tool invocations.
