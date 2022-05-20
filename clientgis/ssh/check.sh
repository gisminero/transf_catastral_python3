if pgrep -x "openvpn" > /dev/null
then
    echo "Running"
else
    echo "Stopped"
fi