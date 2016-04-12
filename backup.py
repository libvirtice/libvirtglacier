import argparse
import datetime
import errno
import logging
import os
import stat
import sys
import time
from xml.etree import ElementTree

import libvirt

from config import ACCESS_KEY_ID, SECRET_ACCESS_KEY, PASSPHRASE, AWS_REGION, COMPRESSION_LEVEL, LOG_FILE

parser = argparse.ArgumentParser()
parser.add_argument('--blockpull',
                    help='Perform a blockpull prior to uploading, so that the resulting disk image is independent of prior snapshots.',
                    action='store_true')
parser.add_argument('--connection', help='Specify qemu:///system or qemu:///session', action='store')
args = parser.parse_args()

from glacier import Glacier

logging.basicConfig(filename=LOG_FILE, level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':
    glacier = Glacier(access_key_id=ACCESS_KEY_ID, secret_access_key=SECRET_ACCESS_KEY, region_name=AWS_REGION)

    try:
        logging.debug("Connecting to libvirt")
        virsh = libvirt.open(args.connection)

        vault = glacier.create_vault()
        for domain in virsh.listAllDomains(flags=libvirt.VIR_CONNECT_LIST_DOMAINS_ACTIVE):
            logging.debug("Parsing XML for: %s" % domain.name())
            domXML = domain.XMLDesc()
            logging.debug(domXML)

            tree = ElementTree.fromstring(domXML)
            disks = tree.findall('//domain/devices/disk[@type="file"]/source/@file')

            if len(disks) == 0:
                continue

            snapshotXML = "<domainsnapshot><disks>"

            for disk_file in disks:
                is_block = stat.S_IFBLK(os.stat(file))
                logging.debug("Disk: %s", disk_file)

                if is_block or disk_file.startswith('/dev/'):
                    # TODO: support block devices? (LVM snapshot?)
                    logging.info('Skipping block device: %s', disk_file)
                    continue

                snapshotXML += "<disk name='%s' snapshot='external'><driver name='qemu' type='qcow2'/></disk>" % disk_file
                if args.blockpull:
                    # issue block pull on the read-only base image, making it independent of any previous snapshots
                    blockJob = domain.blockPull(disk_file)
                    while domain.blockJobInfo(disk_file):
                        time.sleep(30)
                        # TODO use returned jobInfo to wait more appropriately

                snapshotXML += "</disks></domainsnapshot>"

                logging.debug("Creating snapshot...")
                # TODO: get guest agent working for fs quescing
                snapshot = domain.snapshotCreateXML(snapshotXML,
                                                    flags=libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_DISK_ONLY |
                                                          libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_NO_METADATA |  # makes restoring snapshot inconvienant, but no 'cleanup' required this way...
                                                          libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_ATOMIC
                                                    )

                # disks are now the read-only bases for the newly created snapshot disk files.
                logging.info("Snapshot created: %s" % snapshot.getName())

                result = glacier.upload(disk_file, tag=domain.name(), passphrase=PASSPHRASE, compression_level=COMPRESSION_LEVEL)
                if result is False:
                    logging.error("Upload of %s failed", disk_file)
                else:
                    logging.info("Upload of %s completed successfully.")
                    logging.debug(result)

    except Exception as e:
        logging.critical(e)
        sys.exit(errno.ECANCELED)
    else:
        logging.info("%s - No errors occurred during backup." % (datetime.datetime.utcnow()))
