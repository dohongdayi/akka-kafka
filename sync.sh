#!/usr/bin/env bash
rsync -vzrtopg --progress --delete proxy/target/universal/stage/lib/ -e ssh deploy@pcl04:/app/share/deploy/kafka/lib/
rsync -vzrtopg --progress --delete proxy/target/universal/stage/bin/ -e ssh deploy@pcl04:/app/share/deploy/kafka/bin/
