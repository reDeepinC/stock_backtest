import os
import cfg

if __name__ == '__main__':
    os.makedirs(cfg.snapshot_dir, exist_ok=True)
    os.makedirs(cfg.prem_dir, exist_ok=True)
    import download_raw_snapshot_forsim
    download_raw_snapshot_forsim.main()

    import generate_prem
    generate_prem.main()

    import simulator
    simulator.main()

    import sim_append
    sim_append.main()

    import select_args
    select_args.main()