#!/bin/bash
# ==============================================================================
# Real-Time Sentiment Analysis - Full Pipeline Runner for WSL Ubuntu
# ==============================================================================

set -e

# Set Colors
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${CYAN}================================================================${NC}"
echo -e "${CYAN}   🚀 STARTING REAL-TIME SENTIMENT ANALYSIS PIPELINE (WSL)      ${NC}"
echo -e "${CYAN}================================================================${NC}"

# Navigate to project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"
echo -e "${GREEN}[*] Project Directory:${NC} $PROJECT_DIR"

# 1. Activate Python virtual environment if available
if [ -d "/home/saumya/spark_env" ]; then
    echo -e "${GREEN}[*] Activating spark_env...${NC}"
    source /home/saumya/spark_env/bin/activate
elif [ -d "venv" ]; then
    echo -e "${GREEN}[*] Activating local venv...${NC}"
    source venv/bin/activate
fi

# 2. Check / Start Kafka Broker
echo -e "\n${YELLOW}[1/4] Checking Kafka Status...${NC}"
if nc -z localhost 9092 2>/dev/null; then
    echo -e "${GREEN}✅ Kafka Broker is already running on port 9092${NC}"
else
    echo -e "${YELLOW}⚠️ Kafka is not running. Attempting to start Kafka from /opt/kafka or ~/kafka...${NC}"
    if [ -d "/opt/kafka" ]; then
        /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties || true
    elif [ -d "/home/saumya/kafka_2.13-4.1.0" ]; then
        /home/saumya/kafka_2.13-4.1.0/bin/kafka-server-start.sh -daemon /home/saumya/kafka_2.13-4.1.0/config/server.properties || true
    else
        echo -e "${YELLOW}ℹ️ Kafka not found in standard paths; starting in standalone streaming mode.${NC}"
    fi
fi

# 3. Create Kafka Topics if kafka cli exists
if command -v kafka-topics.sh &> /dev/null; then
    echo -e "\n${YELLOW}[2/4] Ensuring Kafka Topics exist...${NC}"
    kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic sentiment_inputs --partitions 1 --replication-factor 1 || true
    kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic sentiment_results --partitions 1 --replication-factor 1 || true
elif [ -f "/opt/kafka/bin/kafka-topics.sh" ]; then
    /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic sentiment_inputs --partitions 1 --replication-factor 1 || true
    /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic sentiment_results --partitions 1 --replication-factor 1 || true
fi

# 4. Start Live Sentiment Dashboard Server
echo -e "\n${YELLOW}[3/4] Starting Dashboard Server on http://localhost:8050 ...${NC}"
mkdir -p logs
python3 dashboard/server.py > logs/dashboard.log 2>&1 &
DASHBOARD_PID=$!
echo -e "${GREEN}✅ Dashboard started! (PID: $DASHBOARD_PID)${NC}"
echo -e "${CYAN}👉 Open Dashboard in Browser: http://localhost:8050 ${NC}"

# 5. Launch Kafka Live Producer
echo -e "\n${YELLOW}[4/4] Starting Kafka Sentiment Stream Producer...${NC}"
echo -e "${GREEN}Press Ctrl+C anytime to stop the pipeline.${NC}\n"

trap "echo -e '\n${RED}🛑 Stopping all services...${NC}'; kill $DASHBOARD_PID 2>/dev/null || true; exit 0" INT TERM

python3 kafka/producer.py 2.0
