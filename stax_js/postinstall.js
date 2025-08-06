const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');




if (fs.existsSync(path.join(__dirname, 'binding.gyp'))) {
    console.log('[StaxDB] Development environment detected (binding.gyp found).');
    console.log('[StaxDB] Compiling from source...');
    try {
        
        execSync('node-gyp rebuild', { cwd: __dirname, stdio: 'inherit' });
        console.log('[StaxDB] Source compilation successful.');
        process.exit(0); 
    } catch (error) {
        console.error('[StaxDB] ERROR: Source compilation failed.');
        
        process.exit(1); 
    }
}



console.log('[StaxDB] Installing from pre-compiled binaries...');

function getPlatformIdentifier() {
    const os = process.platform;
    const arch = process.arch;
    
    
    if (os === 'win32' && arch === 'x64') return 'win32-x64';
    if (os === 'darwin' && arch === 'x64') return 'darwin-x64';
    if (os === 'darwin' && arch === 'arm64') return 'darwin-arm64';
    if (os === 'linux' && arch === 'x64') {
        
        try {
            const lddOutput = execSync('ldd --version').toString();
            if (lddOutput.includes('musl')) {
                return 'linux-x64-musl';
            }
        } catch (e) {
            
        }
        return 'linux-x64-glibc';
    }
    
    

    throw new Error(`Unsupported platform: ${os}-${arch}`);
}

function findAndCopyBinary() {
    try {
        const platform = getPlatformIdentifier();
        console.log(`[StaxDB] Detected platform: ${platform}`);

        const prebuildDir = path.join(__dirname, 'prebuilds');
        const binaryName = 'staxdb.node';
        const sourceBinaryPath = path.join(prebuildDir, platform, binaryName);

        if (!fs.existsSync(sourceBinaryPath)) {
            console.error(`[StaxDB] ERROR: Pre-compiled binary not found for your platform (${platform}).`);
            console.error(`[StaxDB] This package is distributed with binaries only.`);
            console.error(`[StaxDB] Please open an issue at the repository if you believe your platform should be supported.`);
            process.exit(1);
        }

        const buildDir = path.join(__dirname, 'build', 'Release');
        const destBinaryPath = path.join(buildDir, binaryName);

        if (!fs.existsSync(buildDir)) {
            fs.mkdirSync(buildDir, { recursive: true });
        }

        fs.copyFileSync(sourceBinaryPath, destBinaryPath);
        console.log(`[StaxDB] Native binary copied to ${destBinaryPath}`);
        console.log('[StaxDB] Installation successful.');

    } catch (error) {
        console.error('[StaxDB] An error occurred during post-installation setup:');
        console.error(error);
        process.exit(1);
    }
}


findAndCopyBinary();