# find services with rtl in them


systemctl --user list-units --all | grep -i rtl
systemctl --user list-unit-files | grep -i rtl

# service daemon reload
systemctl --user daemon-reload

# service restart
systemctl --user restart scanner-websocket.service

# service start
systemctl --user start scanner-websocket.service

# service stop
systemctl --user stop scanner-websocket.service

# service status
systemctl --user status scanner-websocket.service


