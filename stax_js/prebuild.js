const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');

console.log('--- Starting pre-build process for NPM publish ---');

try {
    
    console.log('Step 1: Cleaning previous build artifacts...');
    if (fs.existsSync(path.join(__dirname, 'build'))) {
        fs.rmSync(path.join(__dirname, 'build'), { recursive: true, force: true });
    }
    console.log('Clean complete.');

    
    console.log('Step 2: Running node-gyp rebuild...');
    
    execSync('npm install --ignore-scripts', { cwd: __dirname, stdio: 'inherit' });
    execSync('node-gyp rebuild', { cwd: __dirname, stdio: 'inherit' });
    console.log('Rebuild successful.');

    
    console.log('Step 3: Staging the pre-built binary...');
    const platform = os.platform();
    const arch = os.arch();
    let platformDir = `${platform}-${arch}`;

    
    if (platform === 'linux') {
        try {
            const lddOutput = execSync('ldd --version 2>/dev/null').toString();
            if (lddOutput.includes('musl')) {
                platformDir += '-musl';
            }
        } catch (e) {
            
        }
    }
    
    console.log(`Detected platform: ${platformDir}`);

    
    const sourceBinary = path.join(__dirname, 'build', 'Release', 'staxdb.node');
    const destDir = path.join(__dirname, 'prebuilds', platformDir);
    const destBinary = path.join(destDir, 'staxdb.node');

    if (!fs.existsSync(sourceBinary)) {
        throw new Error(`Compiled binary not found at ${sourceBinary}`);
    }

    fs.mkdirSync(destDir, { recursive: true });
    fs.copyFileSync(sourceBinary, destBinary);

    console.log(`Binary successfully copied to ${destBinary}`);
    console.log('--- Pre-build process complete. Package is ready for publishing. ---');
    process.exit(0);

} catch (error) {
    console.error('!!! PRE-BUILD PROCESS FAILED !!!');
    console.error(error.message);
    process.exit(1);
}