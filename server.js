module.exports = {
  apps: [{
    name: 'python-server',
    script: 'server.py',
    interpreter: 'python',
    instances: 4,
    exec_mode: 'cluster',
    env: {
      NODE_ENV: 'production'
    },
    error_file: './logs/error.log',
    out_file: './logs/out.log'
  }]
};